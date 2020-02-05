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
  * This is a class for configuration provided by the DatasetComparisonConfig parameters
  *
  * Note: scopt requires all fields to have default values.
  *       Even if a field is mandatory it needs a default value.
  */
case class FormatConfig(rawFormat: String = "xml",
                                   rowTag: Option[String] = None,
                                   csvDelimiter: String = ",",
                                   csvHeader: Boolean = false)

object FormatConfig {
  def getComparisonArguments(format: String, cmd: DatasetComparisonConfig ): FormatConfig = {
    def createGenericConfig(cmd: DatasetComparisonConfig): FormatConfig = {
      new FormatConfig(
        rawFormat = cmd.rawFormat,
        rowTag = cmd.rowTag,
        csvDelimiter = cmd.csvDelimiter,
        csvHeader = cmd.csvHeader)
    }

    format match {
      case "refRawFormat" => if (cmd.rawFormat.nonEmpty && cmd.refRawFormat.isEmpty) {
        createGenericConfig(cmd)
      } else {
        new FormatConfig(
          rawFormat = cmd.refRawFormat,
          rowTag = cmd.refRowTag,
          csvDelimiter = cmd.refCsvDelimiter,
          csvHeader = cmd.refCsvHeader)
      }
      case "newRawFormat" => if (cmd.rawFormat.nonEmpty && cmd.newRawFormat.isEmpty ) {
        createGenericConfig(cmd)
      } else {
        new FormatConfig(
          rawFormat = cmd.newRawFormat,
          rowTag = cmd.newRowTag,
          csvDelimiter = cmd.newCsvDelimiter,
          csvHeader = cmd.newCsvHeader)
      }
    }
  }
}
