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

package za.co.absa.hermes.datasetComparison.dataFrame

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, SparkSession}
import za.co.absa.hermes.datasetComparison.PathResolver

object Utils {
  private def setOptions(dfReader: DataFrameReader, parameters: Parameters): DataFrameReader =
    if (parameters.options.isEmpty) dfReader else dfReader.options(parameters.options)

  private def setOptions[T](dfReader: DataFrameWriter[T], parameters: Parameters): DataFrameWriter[T] =
    if (parameters.options.isEmpty) dfReader else dfReader.options(parameters.options)

  private def load(dfReader: DataFrameReader, parameters: Parameters): DataFrame =
    if (parameters.format == "jdbc") dfReader.load()
    else dfReader.load(parameters.path)

  private def save[T](dfWriter: DataFrameWriter[T], parameters: Parameters, endPath: String = ""): Unit =
    if (parameters.format == "jdbc") dfWriter.save()
    else dfWriter.save(endPath)

  def getUniqueFilePath(parameters: Parameters, extraPath: String, conf: Configuration): PathResolver = {
    val pathResolver: PathResolver = PathResolver.pathStringToFsWithPath(s"${parameters.path}$extraPath", conf)

    if (pathResolver.fsExists) {
      pathResolver.getPathWithTimestamp
    } else {
      pathResolver
    }
  }

  def loadDataFrame(parameters: Parameters)(implicit spark: SparkSession): DataFrame = {
    val dfReader = spark.read.format(parameters.format)
    val withOptions = setOptions(dfReader, parameters)
    load(withOptions, parameters)
  }

  def writeDataFrame(df: DataFrame, parameters: Parameters, pathSuffix: String = "")
                    (implicit spark: SparkSession): Unit = {
    val dfWriter =  df.write.format(parameters.format)
    val withOptions = setOptions(dfWriter, parameters)
    save(withOptions, parameters,s"${parameters.path}$pathSuffix")
  }

  def writeNextDataFrame(df: DataFrame, parameters: Parameters, pathSuffix: String = "")
                        (implicit spark: SparkSession): PathResolver = {
    val pathResolver = getUniqueFilePath(parameters, pathSuffix, spark.sparkContext.hadoopConfiguration)
    val dfWriter =  df.write.format(parameters.format)
    val withOptions = setOptions(dfWriter, parameters)
    save(withOptions, parameters, pathResolver.fullPath)
    pathResolver
  }
}
