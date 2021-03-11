/***********************************************************************
 * Copyright (c) 2015-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor.validators

import org.apache.nifi.components.{ValidationContext, ValidationResult, Validator}
import org.geomesa.nifi.datastore.processor.mixins.DataStoreIngestProcessor.FeatureWriters

import scala.util.{Success, Try}

/**
  * Validates write mode
  */
object WriteModeValidator extends Validator {

  private val Values = Seq(FeatureWriters.Append, FeatureWriters.Modify)

  override def validate(subject: String, input: String, context: ValidationContext): ValidationResult = {
    val builder = new ValidationResult.Builder().subject(subject).input(input)
    if (context.isExpressionLanguagePresent(input)) {
      builder.explanation("Expression Language Present").valid(true).build()
    } else if (input == null || input.isEmpty || Values.exists(_.equalsIgnoreCase(input))) {
      builder.valid(true).build()
    } else {
      builder.explanation(s"Input must be one of '${Values.mkString("', '")}'").valid(false).build()
    }
  }
}
