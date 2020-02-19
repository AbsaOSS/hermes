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

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import za.co.absa.hermes.utils.HelperFunctions

object DatasetComparisonJob {
  private val conf: Config = ConfigFactory.load()
  private val errorColumnName: String = conf.getString("dataset-comparison.errColumn")
  private val tmpColumnName: String = conf.getString("dataset-comparison.tmpColumn")
  private val comparisonUniqueId: String = conf.getString("dataset-comparison.comparisonUniqueId")
  private val actualPrefix: String = conf.getString("dataset-comparison.actualPrefix")
  private val expectedPrefix: String = conf.getString("dataset-comparison.expectedPrefix")

  def main(args: Array[String]): Unit = {
    val cliOptions = CliOptions.parse(args)

    implicit val sparkSession: SparkSession = SparkSession.builder()
      .appName(s"Dataset comparison - '${cliOptions.referenceOptions.path}' and " +
        s"'${cliOptions.newOptions.path}'")
      .getOrCreate()

    execute(cliOptions)
  }

  /**
    * Execute the comparison
    *
    * @param cliOptions Provided configuration for the comparison
    * @param sparkSession Implicit spark session
    */
  def execute(cliOptions: CliOptions)(implicit sparkSession: SparkSession): Unit = {
    val expectedDf = cliOptions.referenceOptions.loadDataFrame
    val actualDf = cliOptions.newOptions.loadDataFrame

    val expectedSchema: StructType = getSchemaWithoutMetadata(expectedDf.schema)
    val actualSchema: StructType = getSchemaWithoutMetadata(actualDf.schema)

    if (cliOptions.keys.isDefined) {
      checkForDuplicateRows(actualDf, cliOptions.keys.get.toSeq, cliOptions.outPath)
    }

    if (!SchemaUtils.isSameSchema(expectedSchema, actualSchema)) {
      val diffSchema = actualSchema.diff(expectedSchema) ++ expectedSchema.diff(actualSchema)
      throw SchemasDifferException(cliOptions.referenceOptions.path, cliOptions.newOptions.path, diffSchema)
    }

    val selector: List[Column] = SchemaUtils.getDataFrameSelector(expectedSchema)
    val actualDFSorted = SchemaUtils.alignSchema(actualDf, selector)
    val expectedDFSorted = SchemaUtils.alignSchema(expectedDf, selector)

    val expectedExceptActual: DataFrame = expectedDFSorted.except(actualDFSorted)
    val actualExceptExpected: DataFrame = actualDFSorted.except(expectedDFSorted)

    if ((expectedExceptActual.count() + actualExceptExpected.count()) == 0) {
      scribe.info("Expected and actual data sets are the same.")
    } else {
      cliOptions.keys match {
        case Some(keys) =>
          handleKeyBasedErrorsPresent(keys.toSeq, cliOptions.outPath, expectedExceptActual, actualExceptExpected)
        case None =>
          expectedExceptActual.write.format("parquet").save(s"${cliOptions.outPath}/expected_minus_actual")
          actualExceptExpected.write.format("parquet").save(s"${cliOptions.outPath}/actual_minus_expected")
      }

      throw DatasetsDifferException(cliOptions.referenceOptions.path,
                                    cliOptions.newOptions.path,
                                    cliOptions.outPath,
                                    expectedDf.count(),
                                    actualDf.count())
    }
  }

  /**
    * Renames all columns expect the keys and appends prefix to them.
    *
    * @param dataSet Dataset that needs columns renamed
    * @param keys Keys that are not to be renamed
    * @param prefix Prefix that will be put in front of column names
    * @return New DataFrame with renamed columns
    */
  private def renameColumns(dataSet: DataFrame, keys: Seq[String], prefix: String): DataFrame = {
    val renamedColumns = dataSet.columns.map { column =>
      if (keys.contains(column)) {
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
    * @param keys Keys on which data sets are joined
    * @return Returns new data frame containing both data frames with renamed columns and joined on keys.
    */
  private def joinTwoDataFrames(expected: DataFrame, actual: DataFrame, keys: Seq[String]): DataFrame = {
    val dfNewExpected = renameColumns(expected, keys, expectedPrefix)
    val dfNewColumnsActual = renameColumns(actual, keys, actualPrefix)
    dfNewExpected.join(dfNewColumnsActual, keys,"full")
  }

  /**
    * Checks for duplicate rows based on combination of keys. If a set of keys is present more then once, then data
    * is evaluated as having duplicates and error is thrown.
    *
    * @param df Data frame that will be evaluated for duplicate rows
    * @param keys Set of keys that will be checked
    * @param path Path where the duplicate rows will be written to
    */
  private def checkForDuplicateRows(df: DataFrame, keys: Seq[String], path: String): Unit = {
    val duplicates = df.groupBy(keys.head, keys.tail: _*).count().filter("`count` >= 2")
    if (duplicates.count() > 0) {
      duplicates.write.format("parquet").save(path)
      throw DuplicateRowsInDF(path)
    }
  }

  /**
    * Creates and writes a parquet file that has the original data and differences that were found.
    *
    * @param keys Unique keys of the data frames
    * @param path Path where the difference will be written to
    * @param expectedMinusActual Relative complement of expected and actual data sets
    * @param actualMinusExpected Relative complement of actual and expected data sets
    */
  private def handleKeyBasedErrorsPresent(keys: Seq[String],
                                          path: String,
                                          expectedMinusActual: DataFrame,
                                          actualMinusExpected: DataFrame): Unit = {
    // Create a unique hash key from keys specified
    val expectedWithHashKey = expectedMinusActual.withColumn(comparisonUniqueId, md5(concat(keys.map(col): _*)))
    val actualWithHashKey = actualMinusExpected.withColumn(comparisonUniqueId, md5(concat(keys.map(col): _*)))

    val joinedData: DataFrame = joinTwoDataFrames(expectedWithHashKey, actualWithHashKey, Seq(comparisonUniqueId))

    // Flatten data
    val flatteningFormula = HelperFunctions.flattenSchema(expectedWithHashKey)
    val flatExpected: DataFrame = expectedWithHashKey.select(flatteningFormula: _*)
    val flatActual: DataFrame = actualWithHashKey.select(flatteningFormula: _*)

    val joinedFlatDataWithoutErrCol: DataFrame = joinTwoDataFrames(flatExpected, flatActual, Seq(comparisonUniqueId))
    val joinedFlatDataWithErrCol = joinedFlatDataWithoutErrCol.withColumn(errorColumnName, lit(Array[String]()))

    val columns: Array[String] = flatExpected.columns.filterNot(_ == comparisonUniqueId)
    val flatDataWithErrors: DataFrame = findErrorsInDataSets(columns, joinedFlatDataWithErrCol)

    // Using the hash key, join the original data and error column from the flat data.
    val outputData: DataFrame = joinedData.as("df1")
      .join(flatDataWithErrors.as("df2"), Seq(comparisonUniqueId))
      .select("df1.*", s"df2.${errorColumnName}")
      .drop(comparisonUniqueId)

    outputData.write.format("parquet").save(path)
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
