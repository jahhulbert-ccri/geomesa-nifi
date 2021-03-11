/***********************************************************************
 * Copyright (c) 2015-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.geomesa.nifi.datastore.processor

import java.io.InputStream

import org.apache.nifi.annotation.behavior.{ReadsAttribute, ReadsAttributes}
import org.apache.nifi.annotation.documentation.CapabilityDescription
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor._
import org.apache.nifi.processor.io.InputStreamCallback
import org.apache.nifi.processor.util.StandardValidators
import org.geomesa.nifi.datastore.processor.CompatibilityMode.CompatibilityMode
import org.geomesa.nifi.datastore.processor.mixins.DataStoreIngestProcessor
import org.geomesa.nifi.datastore.processor.mixins.DataStoreIngestProcessor.FeatureWriters
import org.geomesa.nifi.datastore.processor.mixins.FeatureTypeProcessor.Attributes.SftNameAttribute
import org.geotools.data.DataStore
import org.geotools.util.Converters
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.avro.AvroDataFileReader
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.util.Try
import scala.util.control.NonFatal

/**
  * Avro ingest processor for geotools data stores
  */
@CapabilityDescription("Ingest GeoAvro files into GeoMesa")
@ReadsAttributes(
  Array(new ReadsAttribute(attribute = "geomesa.sft.name", description = "GeoMesa SimpleFeatureType name"))
)
trait AvroIngestProcessor extends DataStoreIngestProcessor {

  import AvroIngestProcessor.Properties.UseProvidedFid
  import AvroIngestProcessor.{ModeMappings, Properties}
  import org.geomesa.nifi.datastore.processor.mixins.FeatureTypeProcessor.Properties.FeatureNameOverride

  override protected def getPrimaryProperties: Seq[PropertyDescriptor] =
    super.getPrimaryProperties ++ Seq(FeatureNameOverride, UseProvidedFid)

  // noinspection ScalaDeprecation
  override protected def getConfigProperties: Seq[PropertyDescriptor] =
    super.getConfigProperties ++ Seq(Properties.AvroMatchMode, Properties.AvroSftName)

  // noinspection ScalaDeprecation
  override protected def createIngest(
      context: ProcessContext,
      dataStore: DataStore,
      writers: FeatureWriters,
      mode: CompatibilityMode): IngestProcessor = {
    val useProvidedFid = context.getProperty(UseProvidedFid).getValue.toBoolean
    val matchMode =
      Option(context.getProperty(Properties.AvroMatchMode).getValue)
          .collect { case m if m.nonEmpty && !ModeMappings.contains(m -> mode) =>
            val compat = ModeMappings.collectFirst { case (old, c) if old == m => c }.get
            logger.warn(s"Using deprecated match mode to override compatibility mode $mode with $compat")
            compat
          }

    new AvroIngest(dataStore, writers, matchMode.getOrElse(mode), useProvidedFid)
  }

  /**
   * GeoAvro ingest
   *
   * @param store data store
   * @param writers feature writers
   * @param mode field match mode
   * @param useProvidedFid use provided fid
   */
  class AvroIngest(
      store: DataStore,
      writers: FeatureWriters,
      mode: CompatibilityMode,
      useProvidedFid: Boolean
    ) extends IngestProcessor(store, writers, mode) {

    override def ingest(
        context: ProcessContext,
        session: ProcessSession,
        file: FlowFile,
        fileName: String): IngestResult = {

      var success = 0L
      var failure = 0L

      session.read(file, new InputStreamCallback {
        override def process(in: InputStream): Unit = {
          val reader = new AvroDataFileReader(in)
          try {
            // noinspection ScalaDeprecation
            val nameArg = Option(file.getAttribute(SftNameAttribute)).orElse {
              val fno = Option(context.getProperty(FeatureNameOverride).evaluateAttributeExpressions().getValue)
              val old = Option(context.getProperty(Properties.AvroSftName).getValue)
              if (old.isEmpty) {
                fno
              } else if (fno.isEmpty) {
                logger.warn(
                  s"Using deprecated property ${Properties.AvroSftName.getName} - " +
                      s"use ${FeatureNameOverride.getName} instead")
                old
              } else {
                logger.warn(
                  s"Ignoring property ${Properties.AvroSftName.getName}: ${old.get} " +
                      s"in favor of ${FeatureNameOverride.getName}: ${fno.get}")
                fno
              }
            }

            val sft = nameArg match {
              case Some(name) if name != reader.getSft.getTypeName => SimpleFeatureTypes.renameSft(reader.getSft, name)
              case _ => reader.getSft
            }

            val mapper = checkSchemaAndMapping(sft)
            val features = mapper.map(m => reader.map(m.apply)).getOrElse(reader)
            val writer = writers.borrowWriter(sft.getTypeName, file)
            try {
              features.foreach { sf =>
                try {
                  if (useProvidedFid) {
                    sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
                  }
                  writer.apply(sf)
                  success += 1L
                } catch {
                  case NonFatal(e) =>
                    failure += 1L
                    logError(sf, e)
                }
              }
            } finally {
              writers.returnWriter(writer)
            }
          } finally {
            reader.close()
          }
        }
      })

      IngestResult(success, failure)
    }

    private def checkSchemaAndMapping(sft: SimpleFeatureType): Option[SimpleFeature => SimpleFeature] = {
      val existing = Try(store.getSchema(sft.getTypeName)).getOrElse(null)

      if (existing == null) {
        logger.info(s"Creating schema ${sft.getTypeName}: ${SimpleFeatureTypes.encodeType(sft)}")
        store.createSchema(sft)
        None
      } else if (SimpleFeatureTypes.compare(sft, existing) == 0) {
        None
      } else if (mode == CompatibilityMode.Update) {
        logger.info(
          s"Updating schema ${sft.getTypeName}:" +
              s"\n  from: ${SimpleFeatureTypes.encodeType(existing)}" +
              s"\n  to:   ${SimpleFeatureTypes.encodeType(sft)}")
        store.updateSchema(sft.getTypeName, sft)
        None
      } else if (mode == CompatibilityMode.Existing) {
        Some(AvroIngestProcessor.convert(sft, existing))
      } else {
        throw new IllegalArgumentException(
          "Input schema does not match existing type:" +
              s"\n  Input:    ${SimpleFeatureTypes.encodeType(sft)}" +
              s"\n  Existing: ${SimpleFeatureTypes.encodeType(existing)}")
      }
    }
  }
}

object AvroIngestProcessor {

  import scala.collection.JavaConverters._

  // noinspection ScalaDeprecation
  private val ModeMappings =
    Seq(Properties.ExactMatch -> CompatibilityMode.Exact, Properties.LenientMatch -> CompatibilityMode.Existing)

  // noinspection ScalaDeprecation
  object Properties {

    val UseProvidedFid: PropertyDescriptor =
      new PropertyDescriptor.Builder()
          .name("Use provided feature ID")
          .description("Use the feature ID from the Avro record, or generate a new one")
          .required(false)
          .defaultValue("false")
          .allowableValues("true", "false")
          .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
          .build()

    @deprecated
    val ExactMatch = "by attribute number and order"
    @deprecated
    val LenientMatch = "by attribute name"

    @deprecated
    val AvroMatchMode: PropertyDescriptor =
      new PropertyDescriptor.Builder()
          .name("Avro SFT match mode")
          .description("Determines how Avro SFT mismatches are handled (deprecated - use Schema Compatibility)")
          .required(false)
          .allowableValues(ExactMatch, LenientMatch)
          .build()

    @deprecated
    val AvroSftName: PropertyDescriptor =
      new PropertyDescriptor.Builder()
          .name("SftName")
          .description("Feature type name (deprecated - use FeatureNameOverride)")
          .required(false)
          .build()
  }

  /**
   * Creates an adapter from one SFT to another
   *
   * @param in input sft
   * @param out output sft
   * @return
   */
  def convert(in: SimpleFeatureType, out: SimpleFeatureType): SimpleFeature => SimpleFeature = {
    val outGeometryLocalName = Option(out.getGeometryDescriptor).map(_.getLocalName).orNull
    val inToOut: Seq[SimpleFeature => AnyRef] = out.getAttributeDescriptors.asScala.map { outAttributeDescriptor =>
      val outLocalName = outAttributeDescriptor.getLocalName
      in.indexOf(outLocalName) match {
        case inIndex if inIndex >= 0 =>
          val outTypeBinding = outAttributeDescriptor.getType.getBinding
          if (outTypeBinding.isAssignableFrom(in.getType(inIndex).getBinding)) {
            sf: SimpleFeature => sf.getAttribute(inIndex)
          } else {
            sf: SimpleFeature => Converters.convert(sf.getAttribute(inIndex), outTypeBinding).asInstanceOf[AnyRef]
          }
        case _ if outLocalName == outGeometryLocalName => sf: SimpleFeature => sf.getDefaultGeometry
        case _ => _: SimpleFeature => null
      }
    }
    sf: SimpleFeature => {
      val o = ScalaSimpleFeature.create(out, sf.getID, inToOut.map(_.apply(sf)): _*)
      o.getUserData.putAll(sf.getUserData)
      o
    }
  }
}
