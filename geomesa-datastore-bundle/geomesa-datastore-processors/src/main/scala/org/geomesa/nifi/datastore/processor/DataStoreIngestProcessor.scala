/***********************************************************************
 * Copyright (c) 2015-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.geomesa.nifi.datastore.processor

import java.io.Closeable
import java.util.concurrent.TimeUnit

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool, GenericObjectPoolConfig}
import org.apache.commons.pool2.{BasePooledObjectFactory, ObjectPool, PooledObject}
import org.apache.nifi.annotation.behavior.{WritesAttribute, WritesAttributes}
import org.apache.nifi.annotation.lifecycle._
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.expression.ExpressionLanguageScope
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor._
import org.apache.nifi.processor.util.StandardValidators
import org.apache.nifi.util.FormatUtils
import org.geomesa.nifi.datastore.processor.CompatibilityMode.CompatibilityMode
import org.geomesa.nifi.datastore.processor.DataStoreIngestProcessor.FeatureWriters.{AppendWriter, ModifyWriter, SimpleWriter}
import org.geomesa.nifi.datastore.processor.DataStoreIngestProcessor.{FeatureWriters, ModifyWriters, PooledWriters, SingletonWriters}
import org.geotools.data._
import org.geotools.filter.text.ecql.ECQL
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore.SchemaCompatibility
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.{CloseWithLogging, WithClose}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
  * Abstract ingest processor for geotools data stores
  */
@WritesAttributes(
  Array(
    new WritesAttribute(attribute = "geomesa.ingest.successes", description = "Number of features written successfully"),
    new WritesAttribute(attribute = "geomesa.ingest.failures", description = "Number of features with errors")
  )
)
trait DataStoreIngestProcessor extends DataStoreProcessor {

  import DataStoreIngestProcessor.Properties._
  import Relationships.{FailureRelationship, SuccessRelationship}

  import scala.collection.JavaConverters._

  @volatile
  private var ingest: IngestProcessor = _

  @OnAdded // reload on add to pick up any sft/converter classpath changes
  def reloadDescriptors(): Unit = loadDescriptors()

  @OnScheduled
  def initialize(context: ProcessContext): Unit = {
    logger.info("Initializing")

    val dataStore = loadDataStore(context)

    try {
      val appender = if (context.getProperty(FeatureWriterCaching).getValue.toBoolean) {
        val timeout = context.getProperty(FeatureWriterCacheTimeout).evaluateAttributeExpressions().getValue
        new PooledWriters(dataStore, FormatUtils.getPreciseTimeDuration(timeout, TimeUnit.MILLISECONDS).toLong)
      } else {
        new SingletonWriters(dataStore)
      }

      val writers =
        if (context.getProperty(WriteMode).getValue == FeatureWriters.Modify) {
          val attribute = Option(context.getProperty(ModifyAttribute).evaluateAttributeExpressions().getValue)
          new ModifyWriters(dataStore, attribute, appender)
        } else {
          appender
        }

      try {
        ingest = createIngest(context, dataStore, writers)
      } catch {
        case NonFatal(e) => writers.close(); throw e
      }
    } catch {
      case NonFatal(e) => dataStore.dispose(); throw e
    }

    logger.info(s"Initialized datastore ${dataStore.getClass.getSimpleName} with ingest ${ingest.getClass.getSimpleName}")
  }

  override def onTrigger(context: ProcessContext, session: ProcessSession): Unit = {
    val flowFiles = session.get(context.getProperty(NifiBatchSize).evaluateAttributeExpressions().asInteger())
    logger.debug(s"Processing ${flowFiles.size()} files in batch")
    if (flowFiles != null && flowFiles.size > 0) {
      flowFiles.asScala.foreach { f =>
        try {
          logger.debug(s"Processing file ${fullName(f)}")
          ingest.ingest(context, session, f)
          session.transfer(f, SuccessRelationship)
        } catch {
          case NonFatal(e) =>
            logger.error(s"Error processing file ${fullName(f)}:", e)
            session.transfer(f, FailureRelationship)
        }
      }
    }
  }

  @OnRemoved
  @OnStopped
  @OnShutdown
  def cleanup(): Unit = {
    logger.info("Processor shutting down")
    val start = System.currentTimeMillis()
    if (ingest != null) {
      CloseWithLogging(ingest)
      ingest = null
    }
    logger.info(s"Shut down in ${System.currentTimeMillis() - start}ms")
  }

  override protected def getConfigProperties: Seq[PropertyDescriptor] =
    super.getConfigProperties ++ Seq(WriteMode, ModifyAttribute, FeatureWriterCaching, FeatureWriterCacheTimeout)

  /**
   * Provides the processor a chance to configure a feature type before it's created in the data store
   *
   * @param sft simple feature type
   */
  protected def decorate(sft: SimpleFeatureType): SimpleFeatureType = sft

  protected def createIngest(context: ProcessContext, dataStore: DataStore, writers: FeatureWriters): IngestProcessor

  /**
   * Abstraction over ingest methods
   *
   * @param store data store
   * @param writers feature writers
   */
  abstract class IngestProcessor(store: DataStore, writers: FeatureWriters, mode: CompatibilityMode)
      extends Closeable {

    /**
     * Ingest a flow file
     *
     * @param context context
     * @param session session
     * @param file flow file
     */
    def ingest(context: ProcessContext, session: ProcessSession, file: FlowFile): Unit = {
      val fullFlowFileName = fullName(file)
      logger.debug(s"Running ${getClass.getName} on file $fullFlowFileName")

      val (success, failure) = ingest(context, session, file, fullFlowFileName)

      session.putAttribute(file, Attributes.IngestSuccessCount, success.toString)
      session.putAttribute(file, Attributes.IngestFailureCount, failure.toString)
      logger.debug(s"Ingested file $fullFlowFileName with $success successes and $failure failures")
    }

    override def close(): Unit = store.dispose()

    /**
     * Ingest a flow file
     *
     * @param context context
     * @param session session
     * @param file flow file
     * @param fileName file name
     * @return (success count, failure count)
     */
    protected def ingest(
        context: ProcessContext,
        session: ProcessSession,
        file: FlowFile,
        fileName: String): (Long, Long)

    /**
     * Check and update the schema in the data store, as needed
     *
     * @param sft simple feature type
     */
    protected def checkSchema(sft: SimpleFeatureType): Unit = {
      store match {
        case gm: GeoMesaDataStore[_] =>
          gm.checkSchemaCompatibility(sft.getTypeName, sft) match {
            case SchemaCompatibility.Unchanged => // no-op

            case c: SchemaCompatibility.DoesNotExist =>
              logger.info(s"Creating schema ${sft.getTypeName}: ${SimpleFeatureTypes.encodeType(sft)}")
              c.apply() // create the schema

            case c: SchemaCompatibility.Compatible =>
              mode match {
                case CompatibilityMode.Exact =>
                  logger.error(
                    s"Detected schema change ${sft.getTypeName}:" +
                        s"\n  from ${SimpleFeatureTypes.encodeType(store.getSchema(sft.getTypeName))} " +
                        s"\n  to ${SimpleFeatureTypes.encodeType(sft)}")
                  throw new RuntimeException(
                    s"Detected schema change for ${sft.getTypeName} but compatibility mode is set to 'exact'")

                case CompatibilityMode.Existing =>
                  logger.warn(
                    s"Detected schema change ${sft.getTypeName}:" +
                        s"\n  from ${SimpleFeatureTypes.encodeType(store.getSchema(sft.getTypeName))} " +
                        s"\n  to ${SimpleFeatureTypes.encodeType(sft)}")

                case CompatibilityMode.Update =>
                  logger.info(
                    s"Updating schema ${sft.getTypeName}:" +
                        s"\n  from ${SimpleFeatureTypes.encodeType(store.getSchema(sft.getTypeName))} " +
                        s"\n  to ${SimpleFeatureTypes.encodeType(sft)}")
                  c.apply() // update the schema
                  writers.invalidate(sft.getTypeName) // invalidate the writer cache
              }

            case c: SchemaCompatibility.Incompatible =>
              logger.error(s"Incompatible schema change detected for schema ${sft.getTypeName}")
              c.apply() // re-throw the error
          }

        case _ =>
          Try(store.getSchema(sft.getTypeName)).filter(_ != null) match {
            case Failure(_) =>
              logger.info(s"Creating schema ${sft.getTypeName}: ${SimpleFeatureTypes.encodeType(sft)}")
              store.createSchema(sft)

            case Success(existing) =>
              if (SimpleFeatureTypes.compare(existing, sft) != 0) {
                mode match {
                  case CompatibilityMode.Exact =>
                    logger.error(
                      s"Detected schema change ${sft.getTypeName}:" +
                          s"\n  from ${SimpleFeatureTypes.encodeType(store.getSchema(sft.getTypeName))} " +
                          s"\n  to ${SimpleFeatureTypes.encodeType(sft)}")
                    throw new RuntimeException(
                      s"Detected schema change for ${sft.getTypeName} but compatibility mode is set to 'exact'")

                  case CompatibilityMode.Existing =>
                    logger.warn(
                      s"Detected schema change ${sft.getTypeName}:" +
                          s"\n  from ${SimpleFeatureTypes.encodeType(store.getSchema(sft.getTypeName))} " +
                          s"\n  to ${SimpleFeatureTypes.encodeType(sft)}")

                  case CompatibilityMode.Update =>
                    logger.info(
                      s"Updating schema ${sft.getTypeName}:" +
                          s"\n  from ${SimpleFeatureTypes.encodeType(existing)} " +
                          s"\n  to ${SimpleFeatureTypes.encodeType(sft)}")
                    store.updateSchema(sft.getTypeName, sft)
                    writers.invalidate(sft.getTypeName)
                }
              }
          }
      }
    }
  }
}

object DataStoreIngestProcessor {

  import scala.collection.JavaConverters._

  /**
   * Abstraction over feature writers
   */
  sealed trait FeatureWriters extends Closeable {

    /**
     * Get a feature writer for the given file
     *
     * @param typeName simple feature type name
     * @return
     */
    def borrowWriter(typeName: String): SimpleWriter

    /**
     *
     */
    def returnWriter(writer: SimpleWriter): Unit

    /**
     * Invalidate any cached writers
     *
     * @param typeName simple feature type name
     */
    def invalidate(typeName: String): Unit
  }

  object FeatureWriters {

    val Append = "append"
    val Modify = "modify"

    trait SimpleWriter extends Closeable {
      def typeName: String
      def apply(f: SimpleFeature): Unit
    }

    case class AppendWriter(typeName: String, writer: FeatureWriter[SimpleFeatureType, SimpleFeature])
        extends SimpleWriter {
      override def apply(f: SimpleFeature): Unit = FeatureUtils.write(writer, f)
      override def close(): Unit = CloseWithLogging(writer)
    }

    case class ModifyWriter(ds: DataStore, typeName: String, attribute: Option[String], append: SimpleWriter)
        extends SimpleWriter with LazyLogging  {
      override def apply(f: SimpleFeature): Unit = {
        val filter = attribute match {
          case None => FilterHelper.ff.id(FilterHelper.ff.featureId(f.getID))
          case Some(a) => FilterHelper.ff.equals(FilterHelper.ff.property(a), FilterHelper.ff.literal(f.getAttribute(a)))
        }
        WithClose(ds.getFeatureWriter(typeName, filter, Transaction.AUTO_COMMIT)) { writer =>
          if (writer.hasNext) {
            val toWrite = writer.next()
            toWrite.setAttributes(f.getAttributes)
            toWrite.getUserData.putAll(f.getUserData)
            toWrite.getUserData.put(Hints.PROVIDED_FID, f.getID)
            writer.write()
            if (writer.hasNext) {
              logger.warn(s"Filter '${ECQL.toCQL(filter)}' matched multiple records, only updating the first one")
            }
          } else {
            append.apply(f)
          }
        }
      }
      override def close(): Unit = append.close()
    }
  }

  /**
   * Pooled feature writers, re-used between flow files
   *
   * @param ds datastore
   * @param timeout how long to wait between flushes of cached feature writers, in millis
   */
  class PooledWriters(ds: DataStore, timeout: Long) extends FeatureWriters {

    private val cache = Caffeine.newBuilder().build(
      new CacheLoader[String, ObjectPool[SimpleWriter]] {
        override def load(key: String): ObjectPool[SimpleWriter] = {
          val factory = new BasePooledObjectFactory[SimpleWriter] {
            override def create(): SimpleWriter = AppendWriter(key, ds.getFeatureWriterAppend(key, Transaction.AUTO_COMMIT))
            override def wrap(obj: SimpleWriter): PooledObject[SimpleWriter] = new DefaultPooledObject(obj)
            override def destroyObject(p: PooledObject[SimpleWriter]): Unit = CloseWithLogging(p.getObject)
          }
          val config = new GenericObjectPoolConfig[SimpleWriter]()
          config.setMaxTotal(-1)
          config.setMaxIdle(-1)
          config.setMinIdle(0)
          config.setMinEvictableIdleTimeMillis(timeout)
          config.setTimeBetweenEvictionRunsMillis(math.max(1000, timeout / 5))
          config.setNumTestsPerEvictionRun(10)

          new GenericObjectPool(factory, config)
        }
      }
    )

    override def borrowWriter(typeName: String): SimpleWriter = cache.get(typeName).borrowObject()
    override def returnWriter(writer: SimpleWriter): Unit = cache.get(writer.typeName).returnObject(writer)

    override def invalidate(typeName: String): Unit = {
      val cur = cache.get(typeName)
      cache.invalidate(typeName)
      CloseWithLogging(cur)
    }

    override def close(): Unit = {
      CloseWithLogging(cache.asMap().values().asScala)
      ds.dispose()
    }
  }

  /**
   * Each flow file gets a new feature writer, which is closed after use
   *
   * @param ds datastore
   */
  class SingletonWriters(ds: DataStore) extends FeatureWriters {
    override def borrowWriter(typeName: String): SimpleWriter =
      AppendWriter(typeName, ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT))
    override def returnWriter(writer: SimpleWriter): Unit = CloseWithLogging(writer)
    override def invalidate(typeName: String): Unit = {}
    override def close(): Unit = ds.dispose()
  }

  /**
   * Each record gets an updating writer
   *
   * @param ds datastore
   * @param attribute the unique attribute used to identify the record to update
   * @param appender appending writer factory
   */
  class ModifyWriters(ds: DataStore, attribute: Option[String], appender: FeatureWriters) extends FeatureWriters {
    override def borrowWriter(typeName: String): SimpleWriter =
      ModifyWriter(ds, typeName, attribute, appender.borrowWriter(typeName))
    override def returnWriter(writer: SimpleWriter): Unit = {
      val ModifyWriter(_, _, _, append) = writer
      appender.returnWriter(append)
      CloseWithLogging(writer)
    }
    override def invalidate(typeName: String): Unit = appender.invalidate(typeName)
    override def close(): Unit = CloseWithLogging(appender) // note: also disposes of the datastore
  }

  /**
   * Processor configuration properties
   */
  object Properties {

    val WriteMode: PropertyDescriptor =
      new PropertyDescriptor.Builder()
          .name("write-mode")
          .displayName("Write Mode")
          .description("Use an appending writer (for new features) or a modifying writer (to update existing features)")
          .allowableValues(FeatureWriters.Append, FeatureWriters.Modify)
          .defaultValue(FeatureWriters.Append)
          .build()

    val ModifyAttribute: PropertyDescriptor =
      new PropertyDescriptor.Builder()
          .name("identifying-attribute")
          .displayName("Identifying Attribute")
          .description(
            "When using a modifying writer, the attribute used to uniquely identify the feature. " +
              "If not specified, will use the feature ID")
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
          .build()

    val NifiBatchSize: PropertyDescriptor =
      new PropertyDescriptor.Builder()
          .name("BatchSize")
          .required(false)
          .description("Number of FlowFiles to execute in a single batch")
          .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
          .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
          .defaultValue("5")
          .build()

    val FeatureWriterCaching: PropertyDescriptor =
      new PropertyDescriptor.Builder()
          .name("FeatureWriterCaching")
          .required(false)
          .description("Enable reuse of feature writers between flow files")
          .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
          .allowableValues("true", "false")
          .defaultValue("false")
          .build()

    val FeatureWriterCacheTimeout: PropertyDescriptor =
      new PropertyDescriptor.Builder()
          .name("FeatureWriterCacheTimeout")
          .required(false)
          .description("How often cached feature writers will flushed to the data store")
          .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
          .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
          .defaultValue("5 minutes")
          .build()

    val SchemaCompatibilityMode: PropertyDescriptor =
      new PropertyDescriptor.Builder()
          .name("schema-compatibility")
          .displayName("Schema Compatibility")
          .required(false)
          .description(
            s"Defines how schema changes will be handled. '${CompatibilityMode.Existing}' will use " +
                s"the existing schema and drop any additional fields. '${CompatibilityMode.Update}' will " +
                s"update the existing schema to match the input schema. '${CompatibilityMode.Exact}' requires" +
                "the input schema to match the existing schema.")
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .allowableValues(CompatibilityMode.Existing.toString, CompatibilityMode.Update.toString, CompatibilityMode.Exact.toString)
          .defaultValue(CompatibilityMode.Existing.toString)
          .build()
  }
}
