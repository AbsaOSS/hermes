/*
 * Copyright 2019 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.hermes.datasetComparison

import scopt.OptionParser

import scala.util.{Failure, Success, Try}

/**
  * This is a class for configuration provided by the command line parameters
  *
  * Note: scopt requires all fields to have default values.
  *       Even if a field is mandatory it needs a default value.
  */
case class DatasetComparisonConfig(refRawFormat: String = "xml",
                                   refRowTag: Option[String] = None,
                                   refCsvDelimiter: String = ",",
                                   refCsvHeader: Boolean = false,
                                   refPath: String = "",
                                   newRawFormat: String = "xml",
                                   newRowTag: Option[String] = None,
                                   newCsvDelimiter: String = ",",
                                   newCsvHeader: Boolean = false,
                                   newPath: String = "",
                                   outPath: String = "",
                                   keys: Option[Seq[String]] = None) {
  /**
    * Checks if keys are defined
    * @return True if the keys are defined
    */
  def hasKeysDefined: Boolean = keys.isDefined

  /**
    * Gets the keys
    * @return Return the list of keys
    */
  def getKeys: Seq[String] = keys.get
}

object DatasetComparisonConfig {

  def getCmdLineArguments(args: Array[String]): Try[DatasetComparisonConfig] = {
    val parser = new CmdParser("spark-submit [spark options] TestUtils.jar")

    parser.parse(args, DatasetComparisonConfig()) match {
      case Some(config) => Success(config)
      case _            => Failure(new IllegalArgumentException("Wrong options provided. List can be found above"))
    }
  }

  private class CmdParser(programName: String) extends OptionParser[DatasetComparisonConfig](programName) {
    head("\nDatasets Comparison", "")
    var refRawFormat: Option[String] = None
    var newRawFormat: Option[String] = None
    var newPath: Option[String] = None
    var refPath: Option[String] = None
    var outPath: Option[String] = None

    private val validateFormatAndOption = (rawFormat: Option[String], opt: String, format: String) => {
      if (rawFormat.isDefined && rawFormat.get.equalsIgnoreCase(format)) {
        success
      } else {
        failure(s"The $opt option is supported only for ${format.toUpperCase} raw data format")
      }
    }

    opt[String]("ref-raw-format")
      .required
      .action((value, config) => {
        refRawFormat = Some(value)
        config.copy(refRawFormat = value)})
      .text("format of the raw data (csv, xml, parquet, etc.)")

    opt[String]("ref-row-tag")
      .optional
      .action((value, config) => config.copy(refRowTag = Some(value)))
      .text("use the specific row tag instead of 'ROW' for XML format")
      .validate( _ => validateFormatAndOption(refRawFormat ,"ref-row-tag", "xml") )

    opt[String]("ref-delimiter")
      .optional
      .action((value, config) => config.copy(refCsvDelimiter = value))
      .text("use the specific delimiter instead of ',' for CSV format")
      .validate( _ => validateFormatAndOption(refRawFormat, "delimiter", "csv") )

    opt[Boolean]("ref-header")
      .optional
      .action((value, config) => config.copy(refCsvHeader = value))
      .text("use the header option to consider CSV header")
      .validate( _ => validateFormatAndOption(refRawFormat,"header", "csv") )

    opt[String]("ref-path")
      .required
      .action((value, config) => {
        refPath = Some(value)
        config.copy(refPath = value)})
      .text("Path to supposedly correct data set.")
      .validate(value =>
        if (newPath.isDefined && newPath.get.equals(value)) {
          failure("ref-path and std-path can not be equal")
        } else if (outPath.isDefined && outPath.get.equals(value)) {
          failure("ref-path and out-path can not be equal")
        } else {
          success
        }
      )

    opt[String]("new-raw-format")
      .required
      .action((value, config) => {
        newRawFormat = Some(value)
        config.copy(newRawFormat = value)})
      .text("format of the raw data (csv, xml, parquet, etc.)")

    opt[String]("new-row-tag")
      .optional
      .action((value, config) => config.copy(newRowTag = Some(value)))
      .text("use the specific row tag instead of 'ROW' for XML format")
      .validate( _ => validateFormatAndOption(newRawFormat ,"new-row-tag", "xml") )

    opt[String]("new-delimiter")
      .optional
      .action((value, config) => config.copy(newCsvDelimiter = value))
      .text("use the specific delimiter instead of ',' for CSV format")
      .validate( _ => validateFormatAndOption(newRawFormat, "delimiter", "csv") )

    opt[Boolean]("new-header")
      .optional
      .action((value, config) => config.copy(newCsvHeader = value))
      .text("use the header option to consider CSV header")
      .validate( _ => validateFormatAndOption(newRawFormat,"header", "csv") )

    opt[String]("new-path")
      .required
      .action((value, config) => {
        newPath = Some(value)
        config.copy(newPath = value)})
      .text("Path to the new dataset, just generated and to be tested.")
      .validate(value =>
        if (refPath.isDefined && refPath.get.equals(value)) {
          failure("std-path and ref-path can not be equal")
        } else if (outPath.isDefined && outPath.get.equals(value)) {
          failure("std-path and out-path can not be equal")
        } else {
          success
        }
      )

    opt[String]("out-path")
      .required
      .action((value, config) => {
        outPath = Some(value)
        config.copy(outPath = value)})
      .text(
        """Path to where the `ComparisonJob` will save the differences.
          |This will effectively creat a folder in which you will find two
          |other folders. expected_minus_actual and actual_minus_expected.
          |Both hold parquet data sets of differences. (minus as in is
          |relative complement)""".stripMargin)
      .validate(value =>
        if (newPath.isDefined && newPath.get.equals(value)) {
          failure("out-path and std-path can not be equal")
        } else if (refPath.isDefined && refPath.get.equals(value)) {
          failure("out-path and ref-path can not be equal")
        } else {
          success
        }
      )

    opt[String]("keys")
      .optional
      .action((value, config) => config.copy(keys = Some(value.split(",").toSeq)))
      .text(
        """If there are know unique keys, they can be specified for better
          |output. Keys should be specified one by one, with , (comma)
          |between them.""".stripMargin)

    help("help")
      .text("prints this usage text")
  }
}
