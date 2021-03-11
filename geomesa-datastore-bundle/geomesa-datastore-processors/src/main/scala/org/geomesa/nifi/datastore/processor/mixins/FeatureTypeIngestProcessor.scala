/***********************************************************************
 * Copyright (c) 2015-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor
package mixins

import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor._
import org.geomesa.nifi.datastore.processor.CompatibilityMode.CompatibilityMode
import org.geomesa.nifi.datastore.processor.mixins.DataStoreIngestProcessor.FeatureWriters
import org.geomesa.nifi.datastore.processor.mixins.DataStoreIngestProcessor.FeatureWriters.SimpleWriter
import org.geotools.data._
import org.opengis.feature.simple.SimpleFeatureType

/**
  * Mixin trait for configuring feature types via typesafe config
  */
trait FeatureTypeIngestProcessor extends DataStoreIngestProcessor with FeatureTypeProcessor {

  /**
   * Abstraction over ingest methods
   *
   * @param store data store
   * @param writers feature writers
   */
  abstract class IngestProcessorWithSchema(
      store: DataStore,
      writers: FeatureWriters,
      mode: CompatibilityMode
    ) extends IngestProcessor(store, writers, mode) {

    override def ingest(
        context: ProcessContext,
        session: ProcessSession,
        file: FlowFile,
        flowFileName: String): IngestResult = {
      val sft = loadFeatureType(context, file)
      checkSchema(sft)
      val writer = writers.borrowWriter(sft.getTypeName, file)
      try { ingest(session, file, flowFileName, sft, writer) } finally {
        writers.returnWriter(writer)
      }
    }

    /**
     * Ingest a flow file
     *
     * @param session session
     * @param file flow file
     * @param name file name
     * @param sft simple feature type
     * @param writer feature writer
     * @return (success count, failure count)
     */
    protected def ingest(
        session: ProcessSession,
        file: FlowFile,
        name: String,
        sft: SimpleFeatureType,
        writer: SimpleWriter): IngestResult
  }
}
