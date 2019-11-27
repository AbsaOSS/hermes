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

import org.apache.spark.sql.{DataFrameReader, SparkSession}

object DataFrameReaderFactory {

  def getDFReaderFromCmdConfig(cmd: DatasetComparisonConfig)
                              (implicit sparkSession: SparkSession): DataFrameReader = {
    cmd.rawFormat match {
      case "csv" => getCsvReader(cmd)
      case "xml" => getXmlReader(cmd)
      case "parquet" => getParquetReader(cmd)
      case _ => getStandardReader(cmd)
    }
  }

  private def getStandardReader(dfReaderOptions: DatasetComparisonConfig)
                               (implicit sparkSession: SparkSession): DataFrameReader = {
    sparkSession.read.format(dfReaderOptions.rawFormat)
  }

  private def getParquetReader(dfReaderOptions: DatasetComparisonConfig)
                              (implicit sparkSession: SparkSession): DataFrameReader = {
    getStandardReader(dfReaderOptions)
  }

  private def getXmlReader(dfReaderOptions: DatasetComparisonConfig)
                          (implicit sparkSession: SparkSession): DataFrameReader = {
    getStandardReader(dfReaderOptions).option("rowTag", dfReaderOptions.rowTag.get)
  }

  private def getCsvReader(dfReaderOptions: DatasetComparisonConfig)
                          (implicit sparkSession: SparkSession): DataFrameReader = {
    getStandardReader(dfReaderOptions)
      .option("delimiter", dfReaderOptions.csvDelimiter)
      .option("header", dfReaderOptions.csvHeader)
  }

}
