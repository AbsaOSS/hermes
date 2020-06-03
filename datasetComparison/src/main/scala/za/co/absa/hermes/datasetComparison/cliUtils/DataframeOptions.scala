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

package za.co.absa.hermes.datasetComparison.cliUtils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql._
import za.co.absa.hermes.datasetComparison.MissingArgumentException

case class DataframeOptions(format: String, options: Map[String, String], path: String) {

  private def setOptions(dfReader: DataFrameReader): DataFrameReader =
    if (options.isEmpty) dfReader else dfReader.options(options)

  private def setOptions[T](dfReader: DataFrameWriter[T]): DataFrameWriter[T] =
    if (options.isEmpty) dfReader else dfReader.options(options)

  private def load(dfReader: DataFrameReader): DataFrame =
    if (format == "jdbc") dfReader.load()
    else dfReader.load(path)

  private def save[T](dfWriter: DataFrameWriter[T], endPath: String = ""): Unit =
    if (format == "jdbc") dfWriter.save()
    else dfWriter.save(endPath)

  def getUniqueFilePath(extraPath: String, conf: Configuration): String = {
    val fs = FileSystem.get(conf)
    val basePath = s"$path$extraPath"

    @scala.annotation.tailrec
    def appendNumberAndTest(namePt1: String,
                            namePt2: String,
                            condition: String => Boolean,
                            count: Int = 1): String = {
      val newName = s"${namePt1}_run$count$namePt2"
      if (condition(newName)) { appendNumberAndTest(namePt1, namePt2, condition, count + 1) }
      else newName
    }

    if (fs.exists(new Path(basePath))) {
      appendNumberAndTest(path, extraPath, { x: String => fs.exists(new Path(x)) })
    } else {
      basePath
    }
  }

  def loadDataFrame(implicit spark: SparkSession): DataFrame = {
    val dfReader = spark.read.format(format)
    val withOptions = setOptions(dfReader)
    load(withOptions)
  }

  def writeDataFrame(df: DataFrame, extraPath: String = "")
                    (implicit spark: SparkSession): Unit = {
    val dfWriter =  df.write.format(format)
    val withOptions = setOptions(dfWriter)
    save(withOptions, s"$path$extraPath")
  }

  def writeNextDataFrame(df: DataFrame, extraPath: String = "")
                        (implicit spark: SparkSession): String = {
    val nonExistingPath: String = getUniqueFilePath(extraPath, spark.sparkContext.hadoopConfiguration)
    val dfWriter =  df.write.format(format)
    val withOptions = setOptions(dfWriter)
    save(withOptions, nonExistingPath)
    nonExistingPath
  }
}

object DataframeOptions {
  def validateAndCreate(options: Map[String, String]): DataframeOptions = {
    val format = options.getOrElse("format", throw MissingArgumentException(
      """Format is mandatory option. Use
        | "--format"
        |""".stripMargin.replaceAll("[\\r\\n]", "")))
    val path = if (format == "jdbc") {
      options.getOrElse("dbtable", throw MissingArgumentException(
        """DB table name is mandatory option for format
          | type jdbc. Use "--dbtable"
          |""".stripMargin.replaceAll("[\\r\\n]", "")))
    } else {
      options.getOrElse("path", throw MissingArgumentException(
        """Path is mandatory option for all format types except jdbc.
          | Use "--path"
          |""".stripMargin.replaceAll("[\\r\\n]", "")))
    }
    val otherOptions = options -- Set("format", "path")
    DataframeOptions(format, otherOptions, path)
  }

  def validateWithDefaultsAndCreate(options: Map[String, String], defaults: Map[String, String]): DataframeOptions = {
    val finalMap = defaults ++ options
    validateAndCreate(finalMap)
  }
}
