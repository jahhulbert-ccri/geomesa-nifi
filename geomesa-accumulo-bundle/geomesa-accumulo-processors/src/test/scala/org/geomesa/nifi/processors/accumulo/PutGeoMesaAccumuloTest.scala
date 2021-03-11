/***********************************************************************
 * Copyright (c) 2015-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.processors.accumulo

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets
import java.text.SimpleDateFormat
import java.util.{Collections, Date}

import com.typesafe.scalalogging.LazyLogging
import org.apache.nifi.avro.AvroReader
import org.apache.nifi.json.JsonTreeReader
import org.apache.nifi.schema.access.SchemaAccessUtils
import org.apache.nifi.schema.inference.SchemaInferenceUtil
import org.apache.nifi.util.TestRunners
import org.geomesa.nifi.datastore.processor.mixins.DataStoreIngestProcessor.FeatureWriters
import org.geomesa.nifi.datastore.processor.mixins.{ConvertInputProcessor, DataStoreIngestProcessor, FeatureTypeProcessor}
import org.geomesa.nifi.datastore.processor.records.Properties
import org.geomesa.nifi.datastore.processor.{CompatibilityMode, RecordUpdateProcessor, Relationships}
import org.geotools.data.{DataStoreFinder, Transaction}
import org.junit.{Assert, Test}
import org.locationtech.geomesa.accumulo.MiniCluster
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreParams
import org.locationtech.geomesa.convert.ConverterConfigLoader
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.avro.AvroDataFileWriter
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypeLoader, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.text.WKTUtils

class PutGeoMesaAccumuloTest extends LazyLogging {

  import scala.collection.JavaConverters._

  // we use class name to prevent spillage between unit tests
  lazy val root = s"${MiniCluster.namespace}.${getClass.getSimpleName}"

  // note the table needs to be different to prevent tests from conflicting with each other
  lazy val dsParams: Map[String, String] = Map(
    AccumuloDataStoreParams.InstanceIdParam.key -> MiniCluster.cluster.getInstanceName,
    AccumuloDataStoreParams.ZookeepersParam.key -> MiniCluster.cluster.getZooKeepers,
    AccumuloDataStoreParams.UserParam.key       -> MiniCluster.Users.root.name,
    AccumuloDataStoreParams.PasswordParam.key   -> MiniCluster.Users.root.password
  )

  @Test
  def testIngest(): Unit = {
    val catalog = s"${root}Ingest"
    val runner = TestRunners.newTestRunner(new PutGeoMesaAccumulo())
    try {
      dsParams.foreach { case (k, v) => runner.setProperty(k, v) }
      runner.setProperty(AccumuloDataStoreParams.CatalogParam.key, catalog)
      runner.setProperty(FeatureTypeProcessor.Properties.SftNameKey, "example")
      runner.setProperty(ConvertInputProcessor.Properties.ConverterNameKey, "example-csv")
      runner.enqueue(getClass.getClassLoader.getResourceAsStream("example.csv"))
      runner.run()
      runner.assertTransferCount(Relationships.SuccessRelationship, 1)
      runner.assertTransferCount(Relationships.FailureRelationship, 0)
    } finally {
      runner.shutdown()
    }

    val ds = DataStoreFinder.getDataStore((dsParams + (AccumuloDataStoreParams.CatalogParam.key -> catalog)).asJava)
    Assert.assertNotNull(ds)
    try {
      val sft = ds.getSchema("example")
      Assert.assertNotNull(sft)
      val features = SelfClosingIterator(ds.getFeatureSource("example").getFeatures.features()).toList
      logger.debug(features.mkString(";"))
      Assert.assertEquals(3, features.length)
    } finally {
      ds.dispose()
    }
  }

  @Test
  def testIngestConvertAttributes(): Unit = {
    val catalog = s"${root}IngestConvertAttributes"
    val runner = TestRunners.newTestRunner(new PutGeoMesaAccumulo())
    try {
      dsParams.foreach { case (k, v) => runner.setProperty(k, v) }
      runner.setProperty(AccumuloDataStoreParams.CatalogParam.key, catalog)
      runner.setProperty(ConvertInputProcessor.Properties.ConvertFlowFileAttributes, "true")
      val attributes = new java.util.HashMap[String, String]()
      attributes.put(FeatureTypeProcessor.Attributes.SftSpecAttribute, "example")
      attributes.put(ConvertInputProcessor.Attributes.ConverterAttribute, "example-csv-attributes")
      attributes.put("my.flowfile.attribute", "foobar")
      runner.enqueue(getClass.getClassLoader.getResourceAsStream("example.csv"), attributes)
      runner.run()
      runner.assertTransferCount(Relationships.SuccessRelationship, 1)
      runner.assertTransferCount(Relationships.FailureRelationship, 0)
    } finally {
      runner.shutdown()
    }

    val ds = DataStoreFinder.getDataStore((dsParams + (AccumuloDataStoreParams.CatalogParam.key -> catalog)).asJava)
    Assert.assertNotNull(ds)
    try {
      val sft = ds.getSchema("example")
      Assert.assertNotNull(sft)
      val features = SelfClosingIterator(ds.getFeatureSource("example").getFeatures.features()).toList
      logger.debug(features.mkString(";"))
      Assert.assertEquals(3, features.length)
      Assert.assertEquals(Seq("foobar2", "foobar3", "foobar4"), features.map(_.getID).sorted)
    } finally {
      ds.dispose()
    }
  }

  @Test
  def testIngestConfigureAttributes(): Unit = {
    val catalog = s"${root}IngestConfigureAttributes"
    val runner = TestRunners.newTestRunner(new PutGeoMesaAccumulo())
    try {
      dsParams.foreach { case (k, v) => runner.setProperty(k, v) }
      runner.setProperty(AccumuloDataStoreParams.CatalogParam.key, catalog)
      val attributes = new java.util.HashMap[String, String]()
      attributes.put(FeatureTypeProcessor.Attributes.SftSpecAttribute, "example")
      attributes.put(ConvertInputProcessor.Attributes.ConverterAttribute, "example-csv")
      runner.enqueue(getClass.getClassLoader.getResourceAsStream("example.csv"), attributes)
      runner.run()
      runner.assertTransferCount(Relationships.SuccessRelationship, 1)
      runner.assertTransferCount(Relationships.FailureRelationship, 0)
    } finally {
      runner.shutdown()
    }

    val ds = DataStoreFinder.getDataStore((dsParams + (AccumuloDataStoreParams.CatalogParam.key -> catalog)).asJava)
    Assert.assertNotNull(ds)
    try {
      val sft = ds.getSchema("example")
      Assert.assertNotNull(sft)
      val features = SelfClosingIterator(ds.getFeatureSource("example").getFeatures.features()).toList
      logger.debug(features.mkString(";"))
      Assert.assertEquals(3, features.length)
    } finally {
      ds.dispose()
    }
  }

  @Test
  def testIngestConfigureAttributeOverride(): Unit = {
    val catalog = s"${root}IngestConfigureAttributeOverride"
    val runner = TestRunners.newTestRunner(new PutGeoMesaAccumulo())
    try {
      dsParams.foreach { case (k, v) => runner.setProperty(k, v) }
      runner.setProperty(AccumuloDataStoreParams.CatalogParam.key, catalog)
      runner.setProperty(FeatureTypeProcessor.Properties.SftNameKey, "example")
      runner.setProperty(ConvertInputProcessor.Properties.ConverterNameKey, "example-csv")
      val attributes = new java.util.HashMap[String, String]()
      attributes.put(FeatureTypeProcessor.Attributes.SftNameAttribute, "renamed")
      runner.enqueue(getClass.getClassLoader.getResourceAsStream("example.csv"), attributes)
      runner.run()
      runner.assertTransferCount(Relationships.SuccessRelationship, 1)
      runner.assertTransferCount(Relationships.FailureRelationship, 0)
    } finally {
      runner.shutdown()
    }

    val ds = DataStoreFinder.getDataStore((dsParams + (AccumuloDataStoreParams.CatalogParam.key -> catalog)).asJava)
    Assert.assertNotNull(ds)
    try {
      val sft = ds.getSchema("renamed")
      Assert.assertNotNull(sft)
      val features = SelfClosingIterator(ds.getFeatureSource("renamed").getFeatures.features()).toList
      logger.debug(features.mkString(";"))
      Assert.assertEquals(3, features.length)
    } finally {
      ds.dispose()
    }
  }

  @Test
  def testAvroIngest(): Unit = {
    val catalog = s"${root}AvroIngest"
    val runner = TestRunners.newTestRunner(new AvroToPutGeoMesaAccumulo())
    try {
      dsParams.foreach { case (k, v) => runner.setProperty(k, v) }
      runner.setProperty(AccumuloDataStoreParams.CatalogParam.key, catalog)
      runner.setProperty(FeatureTypeProcessor.Properties.FeatureNameOverride, "example")
      runner.setProperty(DataStoreIngestProcessor.Properties.SchemaCompatibilityMode, CompatibilityMode.Exact.toString)
      runner.enqueue(getClass.getClassLoader.getResourceAsStream("example-csv.avro"))

      runner.run()
      runner.assertTransferCount(Relationships.SuccessRelationship, 1)
      runner.assertTransferCount(Relationships.FailureRelationship, 0)

      runner.enqueue(getClass.getClassLoader.getResourceAsStream("bad-example-csv.avro"))
      runner.run()
      runner.assertTransferCount(Relationships.SuccessRelationship, 1)
      runner.assertTransferCount(Relationships.FailureRelationship, 1)
    } finally {
      runner.shutdown()
    }

    val ds = DataStoreFinder.getDataStore((dsParams + (AccumuloDataStoreParams.CatalogParam.key -> catalog)).asJava)
    Assert.assertNotNull(ds)
    try {
      val sft = ds.getSchema("example")
      Assert.assertNotNull(sft)
      val features = SelfClosingIterator(ds.getFeatureSource("example").getFeatures.features()).toList
      logger.debug(features.mkString(";"))
      Assert.assertEquals(3, features.length)
    } finally {
      ds.dispose()
    }
  }

  @Test
  def testAvroIngestByName(): Unit = {
    val catalog = s"${root}AvroIngestByName"

    // Let's make a new Avro file
    val sft2 = SimpleFeatureTypes.createType("test2", "lastseen:Date,newField:Double,age:Int,name:String,*geom:Point:srid=4326")

    val baos = new ByteArrayOutputStream()
    val writer = new AvroDataFileWriter(baos, sft2)
    val sf = new ScalaSimpleFeature(sft2, "sf2-record", Array(new Date(), new java.lang.Double(2.34), new Integer(34), "Ray", WKTUtils.read("POINT(1.2 3.4)")))
    sf.getUserData().put("geomesa.feature.visibility", "admin")
    writer.append(sf)
    writer.flush()
    writer.close()

    val is = new ByteArrayInputStream(baos.toByteArray)

    val runner = TestRunners.newTestRunner(new AvroToPutGeoMesaAccumulo())
    try {
      dsParams.foreach { case (k, v) => runner.setProperty(k, v) }
      runner.setProperty(AccumuloDataStoreParams.CatalogParam.key, catalog)
      runner.setProperty(FeatureTypeProcessor.Properties.FeatureNameOverride, "example")
      runner.setProperty(DataStoreIngestProcessor.Properties.SchemaCompatibilityMode, CompatibilityMode.Existing.toString)
      runner.enqueue(getClass.getClassLoader.getResourceAsStream("example-csv.avro"))

      runner.run()
      runner.assertTransferCount(Relationships.SuccessRelationship, 1)
      runner.assertTransferCount(Relationships.FailureRelationship, 0)

      runner.enqueue(getClass.getClassLoader.getResourceAsStream("bad-example-csv.avro"))
      runner.run()
      runner.assertTransferCount(Relationships.SuccessRelationship, 2)
      runner.assertTransferCount(Relationships.FailureRelationship, 0)

      runner.enqueue(is)
      runner.run()
      runner.assertTransferCount(Relationships.SuccessRelationship, 3)
      runner.assertTransferCount(Relationships.FailureRelationship, 0)
    } finally {
      runner.shutdown()
    }

    val ds = DataStoreFinder.getDataStore(
      (dsParams + (AccumuloDataStoreParams.CatalogParam.key -> catalog) +
        (AccumuloDataStoreParams.AuthsParam.key -> "admin")).asJava
    )
    Assert.assertNotNull(ds)
    try {
      val sft = ds.getSchema("example")
      Assert.assertNotNull(sft)
      val features = SelfClosingIterator(ds.getFeatureSource("example").getFeatures.features()).toList
      val ray = features.find(sf => sf.getAttribute("name") == "Ray")
      Assert.assertTrue(ray.map(!_.getUserData.isEmpty).get)
      logger.debug(features.mkString(";"))
      Assert.assertEquals(7, features.length)
    } finally {
      ds.dispose()
    }
  }

  @Test
  def testRecordIngest(): Unit = {
    val catalog = s"${root}RecordIngest"
    val runner = TestRunners.newTestRunner(new PutGeoMesaAccumuloRecord())
    try {
      dsParams.foreach { case (k, v) => runner.setProperty(k, v) }
      runner.setProperty(AccumuloDataStoreParams.CatalogParam.key, catalog)
      val service = new AvroReader()
      runner.addControllerService("avro-record-reader", service)
      runner.setProperty(service, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, "embedded-avro-schema")
      runner.enableControllerService(service)
      runner.setProperty(Properties.RecordReader, "avro-record-reader")
      runner.setProperty(Properties.FeatureIdCol, "__fid__")
      runner.setProperty(Properties.GeometryCols, "*geom:Point")
      runner.setProperty(Properties.GeometrySerializationDefaultWkt, "WKB")
      runner.setProperty(Properties.VisibilitiesCol, "Vis")
      runner.enqueue(getClass.getClassLoader.getResourceAsStream("example.avro"))

      runner.run()
      runner.assertTransferCount(Relationships.SuccessRelationship, 1)
      runner.assertTransferCount(Relationships.FailureRelationship, 0)
    } finally {
      runner.shutdown()
    }

    val ds = DataStoreFinder.getDataStore((dsParams + (AccumuloDataStoreParams.CatalogParam.key -> catalog)).asJava)
    Assert.assertNotNull(ds)
    try {
      val sft = ds.getSchema("example")
      Assert.assertNotNull(sft)
      Assert.assertEquals(10, sft.getAttributeCount)
      Assert.assertEquals(
        Seq("__version__", "Name", "Age", "LastSeen", "Friends", "Skills", "Lon", "Lat", "geom", "__userdata__"),
        sft.getAttributeDescriptors.asScala.map(_.getLocalName))
      val features = SelfClosingIterator(ds.getFeatureSource("example").getFeatures.features()).toList.sortBy(_.getID)
      logger.debug(features.mkString(";"))
      Assert.assertEquals(3, features.length)
      Assert.assertEquals(
        Seq("02c42fc5e8db91d9b8165c6014a23cb0", "6a6e2089854ec6e20015d1c4857b1d9b", "dbd0cf6fc8b5d3d4c54889a493bd5d12"),
        features.map(_.getID)
      )
      Assert.assertEquals(
        Seq(
          "POINT (-100.23650360107422 23)",
          "POINT (3 -62.22999954223633)",
          "POINT (40.231998443603516 -53.235599517822266)"
        ).map(WKTUtils.read),
        features.map(_.getAttribute("geom"))
      )
      Assert.assertEquals(
        Seq(
          Collections.singletonMap("geomesa.feature.visibility", "user"),
          Collections.singletonMap("geomesa.feature.visibility", "user&admin"),
          Collections.singletonMap("geomesa.feature.visibility", "user")
        ),
        features.map(_.getUserData)
      )
    } finally {
      ds.dispose()
    }
  }

  @Test
  def testRecordIngestFlowFileAttributes(): Unit = {
    val catalog = s"${root}RecordIngestAttributes"
    val runner = TestRunners.newTestRunner(new PutGeoMesaAccumuloRecord())
    try {
      dsParams.foreach { case (k, v) => runner.setProperty(k, v) }
      runner.setProperty(AccumuloDataStoreParams.CatalogParam.key, catalog)
      val service = new AvroReader()
      runner.addControllerService("avro-record-reader", service)
      runner.setProperty(service, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, "embedded-avro-schema")
      runner.enableControllerService(service)
      runner.setProperty(Properties.RecordReader, "avro-record-reader")
      runner.setProperty(Properties.TypeName, "${type-name}")
      runner.setProperty(Properties.FeatureIdCol, "${id-col}")
      runner.setProperty(Properties.GeometryCols, "${geom-cols}")
      runner.setProperty(Properties.GeometrySerializationDefaultWkt, "WKB")
      runner.setProperty(Properties.VisibilitiesCol, "${vis-col}")
      val attributes = new java.util.HashMap[String, String]()
      attributes.put("type-name", "attributes")
      attributes.put("id-col", "__fid__")
      attributes.put("geom-cols", "*geom:Point")
      attributes.put("vis-col", "Vis")
      runner.enqueue(getClass.getClassLoader.getResourceAsStream("example.avro"), attributes)

      runner.run()
      runner.assertTransferCount(Relationships.SuccessRelationship, 1)
      runner.assertTransferCount(Relationships.FailureRelationship, 0)
    } finally {
      runner.shutdown()
    }

    val ds = DataStoreFinder.getDataStore((dsParams + (AccumuloDataStoreParams.CatalogParam.key -> catalog)).asJava)
    Assert.assertNotNull(ds)
    try {
      val sft = ds.getSchema("attributes")
      Assert.assertNotNull(sft)
      Assert.assertEquals(10, sft.getAttributeCount)
      Assert.assertEquals(
        Seq("__version__", "Name", "Age", "LastSeen", "Friends", "Skills", "Lon", "Lat", "geom", "__userdata__"),
        sft.getAttributeDescriptors.asScala.map(_.getLocalName))
      val features = SelfClosingIterator(ds.getFeatureSource("attributes").getFeatures.features()).toList.sortBy(_.getID)
      logger.debug(features.mkString(";"))
      Assert.assertEquals(3, features.length)
      Assert.assertEquals(
        Seq("02c42fc5e8db91d9b8165c6014a23cb0", "6a6e2089854ec6e20015d1c4857b1d9b", "dbd0cf6fc8b5d3d4c54889a493bd5d12"),
        features.map(_.getID)
      )
      Assert.assertEquals(
        Seq(
          "POINT (-100.23650360107422 23)",
          "POINT (3 -62.22999954223633)",
          "POINT (40.231998443603516 -53.235599517822266)"
        ).map(WKTUtils.read),
        features.map(_.getAttribute("geom"))
      )
      Assert.assertEquals(
        Seq(
          Collections.singletonMap("geomesa.feature.visibility", "user"),
          Collections.singletonMap("geomesa.feature.visibility", "user&admin"),
          Collections.singletonMap("geomesa.feature.visibility", "user")
        ),
        features.map(_.getUserData)
      )
    } finally {
      ds.dispose()
    }
  }

  @Test
  def testUpdateIngest(): Unit = {
    val catalog = s"${root}UpdateIngest"
    val runner = TestRunners.newTestRunner(new PutGeoMesaAccumulo())

    def checkResults(ids: Seq[String], names: Seq[String], dates: Seq[Date], geoms: Seq[String]): Unit = {
      val ds = DataStoreFinder.getDataStore((dsParams + (AccumuloDataStoreParams.CatalogParam.key -> catalog)).asJava)
      Assert.assertNotNull(ds)
      try {
        val sft = ds.getSchema("example")
        Assert.assertNotNull(sft)
        val features = SelfClosingIterator(ds.getFeatureSource("example").getFeatures.features()).toList.sortBy(_.getID)
        logger.debug(features.mkString(";"))
        Assert.assertEquals(3, features.length)
        Assert.assertEquals(ids, features.map(_.getID))
        Assert.assertEquals(names, features.map(_.getAttribute("name")))
        Assert.assertEquals(dates, features.map(_.getAttribute("dtg")))
        Assert.assertEquals(geoms, features.map(_.getAttribute("geom").toString))
      } finally {
        ds.dispose()
      }
    }

    val df = new SimpleDateFormat("yyyy-MM-dd")
    try {
      dsParams.foreach { case (k, v) => runner.setProperty(k, v) }
      runner.setProperty(AccumuloDataStoreParams.CatalogParam.key, catalog)
      runner.setProperty(FeatureTypeProcessor.Properties.SftNameKey, "example")
      runner.setProperty(ConvertInputProcessor.Properties.ConverterNameKey, "example-csv")
      runner.enqueue(getClass.getClassLoader.getResourceAsStream("example.csv"))
      runner.run()
      runner.assertTransferCount(Relationships.SuccessRelationship, 1)
      runner.assertTransferCount(Relationships.FailureRelationship, 0)
      checkResults(
        Seq("23623", "26236", "3233"),
        Seq("Harry", "Hermione", "Severus"),
        Seq("2015-05-06", "2015-06-07", "2015-10-23").map(df.parse),
        Seq("POINT (-100.2365 23)", "POINT (40.232 -53.2356)", "POINT (3 -62.23)")
      )
      runner.setProperty(DataStoreIngestProcessor.Properties.WriteMode, FeatureWriters.Modify)
      runner.enqueue(getClass.getClassLoader.getResourceAsStream("example-update.csv"))
      runner.run()
      runner.assertTransferCount(Relationships.SuccessRelationship, 2)
      runner.assertTransferCount(Relationships.FailureRelationship, 0)
      checkResults(
        Seq("23623", "26236", "3233"),
        Seq("Harry Potter", "Hermione Granger", "Severus Snape"),
        Seq("2016-05-06", "2016-06-07", "2016-10-23").map(df.parse),
        Seq("POINT (-100.2365 33)", "POINT (40.232 -43.2356)", "POINT (3 -52.23)")
      )
      // verify update by attribute
      runner.setProperty(DataStoreIngestProcessor.Properties.ModifyAttribute, "age")
      runner.enqueue(getClass.getClassLoader.getResourceAsStream("example-update-2.csv"))
      runner.run()
      runner.assertTransferCount(Relationships.SuccessRelationship, 3)
      runner.assertTransferCount(Relationships.FailureRelationship, 0)
      checkResults(
        Seq("23624", "26236", "3233"),
        Seq("Harry", "Hermione Granger", "Severus Snape"),
        Seq("2016-05-06", "2016-06-07", "2016-10-23").map(df.parse),
        Seq("POINT (-100.2365 33)", "POINT (40.232 -43.2356)", "POINT (3 -52.23)")
      )
    } finally {
      runner.shutdown()
    }
  }

  @Test
  def testAppendThenUpdateIngest(): Unit = {
    val catalog = s"${root}AppendUpdateIngest"
    val runner = TestRunners.newTestRunner(new PutGeoMesaAccumulo())

    def checkResults(ages: Seq[Int], dates: Seq[Date], geoms: Seq[String]): Unit = {
      val ds = DataStoreFinder.getDataStore((dsParams + (AccumuloDataStoreParams.CatalogParam.key -> catalog)).asJava)
      Assert.assertNotNull(ds)
      try {
        val sft = ds.getSchema("example")
        Assert.assertNotNull(sft)
        val features = SelfClosingIterator(ds.getFeatureSource("example").getFeatures.features()).toList.sortBy(_.getID)
        logger.debug(features.mkString(";"))
        Assert.assertEquals(3, features.length)
        Assert.assertEquals(Seq("23623", "26236", "3233"), features.map(_.getID))
        Assert.assertEquals(Seq("Harry", "Hermione", "Severus"), features.map(_.getAttribute("name")))
        Assert.assertEquals(ages, features.map(_.getAttribute("age")))
        Assert.assertEquals(dates, features.map(_.getAttribute("dtg")))
        Assert.assertEquals(geoms, features.map(_.getAttribute("geom").toString))
      } finally {
        ds.dispose()
      }
    }

    val df = new SimpleDateFormat("yyyy-MM-dd")
    try {
      dsParams.foreach { case (k, v) => runner.setProperty(k, v) }
      runner.setProperty(AccumuloDataStoreParams.CatalogParam.key, catalog)
      runner.setProperty(FeatureTypeProcessor.Properties.SftNameKey, "example")
      runner.setProperty(ConvertInputProcessor.Properties.ConverterNameKey, "example-csv")
      runner.setProperty(DataStoreIngestProcessor.Properties.WriteMode, "${geomesa.write.mode}")
      runner.setProperty(DataStoreIngestProcessor.Properties.ModifyAttribute, "${geomesa.update.attribute}")
      runner.enqueue(getClass.getClassLoader.getResourceAsStream("example.csv"))
      runner.run()
      runner.assertTransferCount(Relationships.SuccessRelationship, 1)
      runner.assertTransferCount(Relationships.FailureRelationship, 0)
      checkResults(
        Seq(20, 25, 30),
        Seq("2015-05-06", "2015-06-07", "2015-10-23").map(df.parse),
        Seq("POINT (-100.2365 23)", "POINT (40.232 -53.2356)", "POINT (3 -62.23)")
      )

      runner.enqueue(
        getClass.getClassLoader.getResourceAsStream("example-update-3.csv"),
        Map("geomesa.write.mode" -> "modify", "geomesa.update.attribute" -> "name").asJava)
      runner.run()
      runner.assertTransferCount(Relationships.SuccessRelationship, 2)
      runner.assertTransferCount(Relationships.FailureRelationship, 0)
      checkResults(
        Seq(21, 26, 31),
        Seq("2016-05-06", "2016-06-07", "2016-10-23").map(df.parse),
        Seq("POINT (-100.2365 24)", "POINT (40.232 -52.2356)", "POINT (3 -61.23)")
      )
    } finally {
      runner.shutdown()
    }
  }

  @Test
  def testUpdateRecord(): Unit = {
    val catalog = s"${root}UpdateRecord"
    val runner = TestRunners.newTestRunner(new UpdateGeoMesaAccumuloRecord())

    val df = new SimpleDateFormat("yyyy-MM-dd")
    try {
      val params = dsParams + (AccumuloDataStoreParams.CatalogParam.key -> catalog)
      WithClose(DataStoreFinder.getDataStore(params.asJava)) { ds =>
        // create the sft and ingest 3 features
        val sft = SimpleFeatureTypeLoader.sftForName("example").get
        ds.createSchema(sft)
        val converterConf = ConverterConfigLoader.configForName("example-csv")
        val features = WithClose(SimpleFeatureConverter(sft, converterConf.get)) { converter =>
          WithClose(converter.process(getClass.getClassLoader.getResourceAsStream("example.csv")))(_.toList)
        }
        WithClose(ds.getFeatureWriterAppend("example", Transaction.AUTO_COMMIT)) { writer =>
          features.foreach(FeatureUtils.write(writer, _))
        }

        def checkResults(ids: Seq[String], names: Seq[String], dates: Seq[Date], geoms: Seq[String]): Unit = {
          val results = SelfClosingIterator(ds.getFeatureSource("example").getFeatures.features()).toList.sortBy(_.getID)
          logger.debug(results.mkString(";"))
          Assert.assertEquals(3, results.length)
          Assert.assertEquals(ids, results.map(_.getID))
          Assert.assertEquals(names, results.map(_.getAttribute("name")))
          Assert.assertEquals(dates, results.map(_.getAttribute("dtg")))
          Assert.assertEquals(geoms, results.map(_.getAttribute("geom").toString))
        }

        // verify initial write
        checkResults(
          Seq("23623", "26236", "3233"),
          Seq("Harry", "Hermione", "Severus"),
          Seq("2015-05-06", "2015-06-07", "2015-10-23").map(df.parse),
          Seq("POINT (-100.2365 23)", "POINT (40.232 -53.2356)", "POINT (3 -62.23)")
        )

        // configure the processor to use json
        val service = new JsonTreeReader()
        runner.addControllerService("json-record-reader", service)
        runner.setProperty(service, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaInferenceUtil.INFER_SCHEMA)
        runner.enableControllerService(service)
        params.foreach { case (k, v) => runner.setProperty(k, v) }
        runner.setProperty(Properties.RecordReader, "json-record-reader")
        runner.setProperty(Properties.TypeName, "${type-name}")
        runner.setProperty(Properties.FeatureIdCol, "${id-col}")
        runner.setProperty(RecordUpdateProcessor.Properties.LookupCol, "${id-col}")

        // update one name
        runner.enqueue(
          new ByteArrayInputStream("""{"fid":"23623","name":"Harry Potter"}""".getBytes(StandardCharsets.UTF_8)),
          Map("type-name"-> "example", "id-col" -> "fid").asJava)
        runner.run()
        runner.assertTransferCount(Relationships.SuccessRelationship, 1)
        runner.assertTransferCount(Relationships.FailureRelationship, 0)
        checkResults(
          Seq("23623", "26236", "3233"),
          Seq("Harry Potter", "Hermione", "Severus"),
          Seq("2015-05-06", "2015-06-07", "2015-10-23").map(df.parse),
          Seq("POINT (-100.2365 23)", "POINT (40.232 -53.2356)", "POINT (3 -62.23)")
        )

        // update the name and feature id based on matching on age
        runner.setProperty(RecordUpdateProcessor.Properties.LookupCol, "${match-col}")
        runner.enqueue(
          new ByteArrayInputStream("""{"fid":"26237","name":"Hermione Granger","age":25}""".getBytes(StandardCharsets.UTF_8)),
          Map("type-name"-> "example", "id-col" -> "fid", "match-col" -> "age").asJava)
        runner.run()
        runner.assertTransferCount(Relationships.SuccessRelationship, 2)
        runner.assertTransferCount(Relationships.FailureRelationship, 0)
        checkResults(
          Seq("23623", "26237", "3233"),
          Seq("Harry Potter", "Hermione Granger", "Severus"),
          Seq("2015-05-06", "2015-06-07", "2015-10-23").map(df.parse),
          Seq("POINT (-100.2365 23)", "POINT (40.232 -53.2356)", "POINT (3 -62.23)")
        )
      }
    } finally {
      runner.shutdown()
    }
  }
}
