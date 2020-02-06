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

  def getDFReaderFromCmdConfig(cmd: FormatConfig)
                              (implicit sparkSession: SparkSession): DataFrameReader = {
    cmd.rawFormat match {
      case "csv" => getCsvReader(cmd)
      case "xml" => getXmlReader(cmd)
      case "parquet" => getParquetReader(cmd)
      case "jdbc" => getJdbcReader(cmd)
      case "avro" => getAvroReader(cmd)
      case _ => throw new IllegalArgumentException(s"Unknown raw format ${cmd.rawFormat} specified")
    }
  }

  private def getStandardReader(dfReaderOptions: FormatConfig)
                               (implicit sparkSession: SparkSession): DataFrameReader = {
    sparkSession.read.format(dfReaderOptions.rawFormat)
  }

  private def getJdbcReader(dfReaderOptions: FormatConfig)
                           (implicit sparkSession: SparkSession): DataFrameReader = {
    getStandardReader(dfReaderOptions)
      .option("url", dfReaderOptions.jdbcConnectionString.get)
      .option("user", dfReaderOptions.jdbcUsername.get)
      .option("password", dfReaderOptions.jdbcPassword.get)
  }

  private def getParquetReader(dfReaderOptions: FormatConfig)
                              (implicit sparkSession: SparkSession): DataFrameReader = {
    getStandardReader(dfReaderOptions)
  }

  private def getAvroReader(dfReaderOptions: FormatConfig)
                           (implicit sparkSession: SparkSession): DataFrameReader = {
    getStandardReader(dfReaderOptions)
  }

  private def getXmlReader(dfReaderOptions: FormatConfig)
                          (implicit sparkSession: SparkSession): DataFrameReader = {
    getStandardReader(dfReaderOptions).option("rowTag", dfReaderOptions.rowTag.get)
  }

  private def getCsvReader(dfReaderOptions: FormatConfig)
                          (implicit sparkSession: SparkSession): DataFrameReader = {
    getStandardReader(dfReaderOptions)
      .option("delimiter", dfReaderOptions.csvDelimiter)
      .option("header", dfReaderOptions.csvHeader)
  }

}
