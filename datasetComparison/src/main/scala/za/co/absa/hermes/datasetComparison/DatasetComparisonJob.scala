/*
 * Copyright 2019 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
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
import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, StructType}
import za.co.absa.hermes.datasetComparison.cliUtils.{CliParameters, CliParametersParser}
import za.co.absa.hermes.datasetComparison.config.{DatasetComparisonConfig, TypesafeConfig}
import za.co.absa.hermes.datasetComparison.dataFrame.Utils
import za.co.absa.hermes.utils.SparkCompatibility

object DatasetComparisonJob {

  def main(args: Array[String]): Unit = {
    SparkCompatibility.checkVersion(SPARK_VERSION)

    val cliParameters = CliParametersParser.parse(args)

    implicit val sparkSession: SparkSession = SparkSession.builder()
      .appName(
        s"""Dataset comparison - '${cliParameters.referenceDataParameters.path}' and
           | '${cliParameters.actualDataParameters.path}'
           |""".stripMargin.replaceAll("[\\r\\n]", "")
      )
      .getOrCreate()

    execute(cliParameters, None)
  }

  /**
    * Execute the comparison
    *
    * @param cliParameters Provided configuration for the comparison
    * @param sparkSession Implicit spark session
    */
  def execute(cliParameters: CliParameters, configPath: Option[String] = None)
             (implicit sparkSession: SparkSession): Unit = {
    val config = getConfig(configPath)
    val optionalSchema = getSchema(cliParameters.schemaPath)
    val dataFrameRef = Utils.loadDataFrame(cliParameters.referenceDataParameters)
    val dataFrameActual = Utils.loadDataFrame(cliParameters.actualDataParameters)
    val dsComparison = new DatasetComparator(dataFrameRef, dataFrameActual, cliParameters.keys, config, optionalSchema)
    val result = dsComparison.compare

    val finalPath = cliParameters.outDataParameters.map { outOptions =>
      val path: String = result.resultDF match {
        case Some(df) => Utils.writeNextDataFrame(df, outOptions)
        case None => Utils.getUniqueFilePath(outOptions, "", sparkSession.sparkContext.hadoopConfiguration)
      }
      writeMetricsToFile(result, path)
      path
    }

    if (result.diffCount > 0) {
      throw DatasetsDifferException(
          cliParameters.referenceDataParameters.path,
          cliParameters.actualDataParameters.path,
          finalPath.getOrElse("Not Provided"),
          result.refRowCount,
          result.newRowCount
      )
    } else {
      scribe.info(s"Expected and actual data sets are the same. Metrics written to ${finalPath.getOrElse("Not Provided")}")
    }
  }

  def getSchema(schemaPath: Option[String])(implicit sparkSession: SparkSession): Option[StructType] = {
    schemaPath.map { path =>
      val schemaSource = sparkSession.sparkContext.wholeTextFiles(path).take(1)(0)._2
      DataType.fromJson(schemaSource).asInstanceOf[StructType]
    }
  }

  def getConfig(configPath: Option[String]): DatasetComparisonConfig = {
    val config = new TypesafeConfig(configPath).validate().get
    scribe.info(config.getLoggableString)
    config
  }

  def writeMetricsToFile(result: ComparisonResult, fileName: String)
                        (implicit sparkSession: SparkSession): Unit = {
    val path = new Path(fileName, "_METRICS")
    val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    val fsOut = fs.create(path)

    try {
      val pw = new PrintWriter(fsOut, true)
      pw.print(result.getPrettyJson)
      pw.close()
    } finally {
      fsOut.close()
    }
  }
}
