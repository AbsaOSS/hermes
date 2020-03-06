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

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import za.co.absa.hermes.utils.HelperFunctions

object DatasetComparisonJob {
  case class GenericPair[+T](reference: T, actual: T)

  private val conf: Config = ConfigFactory.load()
  private val errorColumnName: String = conf.getString("dataset-comparison.errColumn")
  private val tmpColumnName: String = conf.getString("dataset-comparison.tmpColumn")
  private val comparisonUniqueId: String = conf.getString("dataset-comparison.comparisonUniqueId")
  private val actualPrefix: String = conf.getString("dataset-comparison.actualPrefix")
  private val expectedPrefix: String = conf.getString("dataset-comparison.expectedPrefix")
  private val allowDuplicates: Boolean = conf.getBoolean("dataset-comparison.allowDuplicates")
  private val deduplicate: Boolean = conf.getBoolean("dataset-comparison.deduplicate")

  def main(args: Array[String]): Unit = {
    val cliOptions = CliOptions.parse(args)

    implicit val sparkSession: SparkSession = SparkSession.builder()
      .appName(
        s"""Dataset comparison - '${cliOptions.referenceOptions.path}' and
           | '${cliOptions.newOptions.path}'
           |""".stripMargin.replaceAll("[\\r\\n]", "")
      )
      .getOrCreate()

    execute(cliOptions, args.mkString(" "))
  }

  /**
    * Execute the comparison
    *
    * @param cliOptions Provided configuration for the comparison
    * @param sparkSession Implicit spark session
    */
  def execute(cliOptions: CliOptions, rawOptions: String = "")(implicit sparkSession: SparkSession): Unit = {
    val result = compare(cliOptions)
    val resultWithRawOpts = result.copy(passedOptions = rawOptions)

    resultWithRawOpts.resultDF.foreach { df => df.write.format("parquet").save(cliOptions.outPath) }

    writeMetricsToFile(resultWithRawOpts, cliOptions.outPath)

    if (resultWithRawOpts.diffCount > 0) {
      throw DatasetsDifferException(
          cliOptions.referenceOptions.path,
          cliOptions.newOptions.path,
          cliOptions.outPath,
          resultWithRawOpts.refRowCount,
          resultWithRawOpts.newRowCount
      )
    } else {
      scribe.info("Expected and actual data sets are the same.")
    }
  }

  def compare(cliOptions: CliOptions)(implicit sparkSession: SparkSession): ComparisonResult = {
    val testedDF = GenericPair(cliOptions.referenceOptions.loadDataFrame, cliOptions.newOptions.loadDataFrame)
    val rowCounts = GenericPair(testedDF.reference.count(), testedDF.actual.count())

    checkSchemas(cliOptions, testedDF.reference, testedDF.actual)

    val selector: List[Column] = SchemaUtils.getDataFrameSelector(testedDF.reference.schema)
    val dfsSorted = GenericPair(
      SchemaUtils.alignSchema(testedDF.reference, selector),
      SchemaUtils.alignSchema(testedDF.actual, selector)
    )

    val dfsWithKey = GenericPair(
      addKeyColumn(cliOptions.keys, selector, dfsSorted.reference),
      addKeyColumn(cliOptions.keys, selector, dfsSorted.actual)
    )

    val (duplicateCounts, dfsDeduplicated) = handleDuplicates(cliOptions.outPath, dfsWithKey)

    val dfsExcepted = GenericPair(
      dfsDeduplicated.reference.except(dfsDeduplicated.actual),
      dfsDeduplicated.actual.except(dfsDeduplicated.reference)
    )

    val exceptedCount = GenericPair(dfsExcepted.reference.count(), dfsExcepted.actual.count())
    val passedCount = rowCounts.reference - exceptedCount.reference

    val resultDF: Option[DataFrame] = (exceptedCount.reference + exceptedCount.actual) match {
      case 0 => None
      case _ => Some(createDiffDataFrame(cliOptions.outPath, dfsExcepted.reference, dfsExcepted.actual))
    }
    val diffCount: Long = resultDF.map(_.count).getOrElse(0)

    ComparisonResult(
      rowCounts.reference,
      rowCounts.actual,
      duplicateCounts.reference,
      duplicateCounts.actual,
      passedCount,
      selector,
      resultDF,
      diffCount
    )
  }

  private def writeMetricsToFile(result: ComparisonResult, fileName: String)(implicit sparkSession: SparkSession): Unit = {
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

  private def handleDuplicates(outPath: String,
                               dfsWithKey: GenericPair[DataFrame]): (GenericPair[Long], GenericPair[DataFrame]) = {
    def writeDown(df: DataFrame, duplicates: DataFrame, path: String): Unit = {
      df.alias("original")
        .join(duplicates, Seq(comparisonUniqueId), "left_outer")
        .select("original.*")
        .write
        .format("parquet")
        .save(path)
    }

    val dfsDuplicates: GenericPair[Option[DataFrame]] = GenericPair(
      checkForDuplicateRows(dfsWithKey.reference),
      checkForDuplicateRows(dfsWithKey.actual)
    )

    val duplicateCounts: GenericPair[Long] = GenericPair(
      dfsDuplicates.reference.map(_.count()).getOrElse(0),
      dfsDuplicates.actual.map(_.count()).getOrElse(0)
    )

    if ((duplicateCounts.reference + duplicateCounts.actual) > 0 && !allowDuplicates) {
      dfsDuplicates.reference.foreach(x => writeDown(dfsWithKey.actual, x, s"$outPath/newDuplicates"))
      dfsDuplicates.actual.foreach(x => writeDown(dfsWithKey.actual, x, s"$outPath/newDuplicates"))

      throw DuplicateRowsInDF(outPath)
    }

    val dfsDeduplicated = if (duplicateCounts.reference + duplicateCounts.actual > 0 && deduplicate) {
      GenericPair(
        dfsWithKey.reference.dropDuplicates(comparisonUniqueId),
        dfsWithKey.actual.dropDuplicates(comparisonUniqueId)
      )
    } else {
      GenericPair(dfsWithKey.reference, dfsWithKey.actual)
    }
    (duplicateCounts, dfsDeduplicated)
  }

  private def checkSchemas(cliOptions: CliOptions, expectedDf: DataFrame, actualDf: DataFrame): Unit = {
    val expectedSchema: StructType = getSchemaWithoutMetadata(expectedDf.schema)
    val actualSchema: StructType = getSchemaWithoutMetadata(actualDf.schema)

    if (!SchemaUtils.isSameSchema(expectedSchema, actualSchema)) {
      val diffSchema = SchemaUtils.diffSchema(expectedSchema, actualSchema) ++
        SchemaUtils.diffSchema(actualSchema, expectedSchema)
      throw SchemasDifferException(cliOptions.referenceOptions.path, cliOptions.newOptions.path, diffSchema.mkString("\n"))
    }
  }

  private def addKeyColumn(keys: Option[Set[String]], selector: List[Column], df: DataFrame): DataFrame = {
    if (keys.isDefined) { df.withColumn(comparisonUniqueId, md5(concat(keys.get.toSeq.map(col): _*))) }
    else { df.withColumn(comparisonUniqueId, md5(concat(selector: _*))) }
  }

  /**
    * Renames all columns expect the keys and appends prefix to them.
    *
    * @param dataSet Dataset that needs columns renamed
    * @param prefix Prefix that will be put in front of column names
    * @return New DataFrame with renamed columns
    */
  private def renameColumns(dataSet: DataFrame, prefix: String): DataFrame = {
    val renamedColumns = dataSet.columns.map { column =>
      if (comparisonUniqueId.equals(column)) {
        dataSet(column)
      } else {
        dataSet(column).as(s"${prefix}_$column")
      }
    }

    dataSet.select(renamedColumns: _*)
  }

  /**
    * Adds prefixes to the two supplied data sets and then joins them using full join on keys. All columns will be
    * renamed based on the prefix passed in through application properties for expected and actual data frames.
    *
    * @param expected Expected data frame
    * @param actual Actual data frame
    * @return Returns new data frame containing both data frames with renamed columns and joined on keys.
    */
  private def joinTwoDataFrames(expected: DataFrame, actual: DataFrame): DataFrame = {
    val dfNewExpected = renameColumns(expected, expectedPrefix)
    val dfNewColumnsActual = renameColumns(actual, actualPrefix)
    dfNewExpected.join(dfNewColumnsActual, Seq(comparisonUniqueId),"full")
  }

  /**
    * Checks for duplicate rows based on combination of keys. If a set of keys is present more then once, then data
    * is evaluated as having duplicates and error is thrown.
    *
    * @param df Data frame that will be evaluated for duplicate rows
    */
  private def checkForDuplicateRows(df: DataFrame): Option[DataFrame] = {
    val duplicates = df.groupBy(comparisonUniqueId).count().filter("`count` >= 2")

    if (duplicates.count() == 0) {
      None
    } else {
      Some(duplicates)
    }
  }

  /**
    * Creates and writes a parquet file that has the original data and differences that were found.
    *
    * @param path Path where the difference will be written to
    * @param expectedMinusActual Relative complement of expected and actual data sets
    * @param actualMinusExpected Relative complement of actual and expected data sets
    */
  private def createDiffDataFrame(path: String,
                                  expectedMinusActual: DataFrame,
                                  actualMinusExpected: DataFrame): DataFrame = {
    val joinedData: DataFrame = joinTwoDataFrames(expectedMinusActual, actualMinusExpected)

    // Flatten data
    val flatteningFormula = HelperFunctions.flattenSchema(expectedMinusActual)
    val flatExpected: DataFrame = expectedMinusActual.select(flatteningFormula: _*)
    val flatActual: DataFrame = actualMinusExpected.select(flatteningFormula: _*)

    val joinedFlatDataWithoutErrCol: DataFrame = joinTwoDataFrames(flatExpected, flatActual)
    val joinedFlatDataWithErrCol = joinedFlatDataWithoutErrCol.withColumn(errorColumnName, lit(Array[String]()))

    val columns: Array[String] = flatExpected.columns.filterNot(_ == comparisonUniqueId)
    val flatDataWithErrors: DataFrame = findErrorsInDataSets(columns, joinedFlatDataWithErrCol)

    // Using the hash key, join the original data and error column from the flat data.
    joinedData.as("df1")
      .join(flatDataWithErrors.as("df2"), Seq(comparisonUniqueId))
      .select("df1.*", s"df2.${errorColumnName}")
      .drop(comparisonUniqueId)
  }

  /**
    * Finds errors in data. Uses columns as a basis for column names and prepends them with {actualPrefix} and
    * {expectedPrefix}. Return a data frame with added error column
    *
    * @param columns column names to be traversed and compared
    * @param joinedFlatDataWithErrCol flattened and joined expected and actual data
    * @return DataFrame with errors in error column
    */
  private def findErrorsInDataSets(columns: Array[String], joinedFlatDataWithErrCol: DataFrame): DataFrame = {
    columns.foldLeft(joinedFlatDataWithErrCol) { (data, column) =>
      data.withColumnRenamed(errorColumnName, tmpColumnName)
        .withColumn(errorColumnName, concat(
          when(col(s"${actualPrefix}_$column") === col(s"${expectedPrefix}_$column") or
            (col(s"${expectedPrefix}_$column").isNull and
              col(s"${actualPrefix}_$column").isNull),
            lit(Array[String]()))
            .otherwise(array(lit(column))), col(tmpColumnName)))
        .drop(tmpColumnName)
    }
  }

  /**
    * Returns data frame schema without metadata
    *
    * @return Schema without metadata
    */
  private def getSchemaWithoutMetadata(schema: StructType): StructType = {
    StructType(schema.map{ f => StructField(f.name, f.dataType, f.nullable) })
  }

}
