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

import java.io.PrintWriter

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, StructType}
import za.co.absa.hermes.datasetComparison.cliUtils.CliOptions
import za.co.absa.hermes.datasetComparison.config.TypesafeConfig

object DatasetComparisonJob {

  def main(args: Array[String]): Unit = {
    val cliOptions = CliOptions.parse(args)

    implicit val sparkSession: SparkSession = SparkSession.builder()
      .appName(
        s"""Dataset comparison - '${cliOptions.referenceOptions.path}' and
           | '${cliOptions.newOptions.path}'
           |""".stripMargin.replaceAll("[\\r\\n]", "")
      )
      .getOrCreate()

    execute(cliOptions, None)
  }

  /**
    * Execute the comparison
    *
    * @param cliOptions Provided configuration for the comparison
    * @param sparkSession Implicit spark session
    */
  def execute(cliOptions: CliOptions, configPath: Option[String] = None)
             (implicit sparkSession: SparkSession): Unit = {
    val config = new TypesafeConfig(configPath)
    val optionalSchema = cliOptions.schemaPath match {
      case Some(schema) =>
        val schemaSource = sparkSession.sparkContext.wholeTextFiles(schema).take(1)(0)._2
        Some(DataType.fromJson(schemaSource).asInstanceOf[StructType])
      case None => None
    }
    val dsComparison = new DatasetComparison(cliOptions, config, optionalSchema)
    val result = dsComparison.compare

    result.resultDF.foreach { df => cliOptions.outOptions.writeDataFrame(df) }

    writeMetricsToFile(result, cliOptions.outOptions.path)

    if (result.diffCount > 0) {
      throw DatasetsDifferException(
          cliOptions.referenceOptions.path,
          cliOptions.newOptions.path,
          cliOptions.outOptions.path,
          result.refRowCount,
          result.newRowCount
      )
    } else {
      scribe.info("Expected and actual data sets are the same.")
    }
  }

  private def writeMetricsToFile(result: ComparisonResult, fileName: String)
                                (implicit sparkSession: SparkSession): Unit = {
    val path = new Path(fileName, "_METRICS")
    val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    val fsOut = fs.create(path)

    try {
      val pw = new PrintWriter(fsOut, true)
      pw.print(result.getJsonMetadata)
      pw.close()
    } finally {
      fsOut.close()
    }
  }
}
