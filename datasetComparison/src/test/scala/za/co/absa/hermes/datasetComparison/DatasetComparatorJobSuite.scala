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

import java.net.URL
import java.nio.file.{Files, Paths}
import java.text.SimpleDateFormat
import java.util.Calendar

import org.scalatest.{BeforeAndAfterEach, FunSuite}
import za.co.absa.hermes.utils.{FileReader, SparkTestBase}

import scala.io.Source

class DatasetComparatorJobSuite extends FunSuite with SparkTestBase with BeforeAndAfterEach {
  val format = new SimpleDateFormat("yyyy_MM_dd-HH_mm_ss")
  var timePrefix = ""

  override def beforeEach(): Unit = {
    timePrefix = format.format(Calendar.getInstance().getTime)
  }

  test("Compare the same datasets by generic way") {
    val outPath = s"target/test_output/comparison_job/positive/$timePrefix"

    val args = Array(
      "--new-format", "csv",
      "--ref-format", "csv",
      "--delimiter", ",",
      "--new-path", getClass.getResource("/dataSample1.csv").toString,
      "--ref-path", getClass.getResource("/dataSample2.csv").toString,
      "--out-path", outPath
    )

    DatasetComparisonJob.main(args)

    assert(Files.exists(Paths.get(outPath,"_METRICS")))
  }

  test("Compare the same datasets by generic way - complex structure") {
    val outPath = s"target/test_output/comparison_job/positive/$timePrefix"

    val args = Array(
      "--format", "json",
      "--new-path", getClass.getResource("/dataSample2.json").toString,
      "--ref-path", getClass.getResource("/dataSample2.json").toString,
      "--out-path", outPath
    )

    DatasetComparisonJob.main(args)

    assert(Files.exists(Paths.get(outPath,"_METRICS")))
  }

  test("Compare different datasets by generic way") {
    val refPath = getClass.getResource("/dataSample1.csv").toString
    val newPath = getClass.getResource("/dataSample3.csv").toString
    val outPath = s"target/test_output/comparison_job/negative/$timePrefix"
    val message = "Expected and actual datasets differ.\n" +
      s"Reference path: $refPath\n" +
      s"Actual dataset path: $newPath\n" +
      s"Difference written to: $outPath\n" +
      "Count Expected( 10 ) vs Actual( 11 )"

    val args = Array(
      "--new-format", "csv",
      "--ref-format", "csv",
      "--delimiter", ",",
      "--new-path", newPath,
      "--ref-path", refPath,
      "--out-path", outPath
    )

    val caught = intercept[DatasetsDifferException] {
      DatasetComparisonJob.main(args)
    }

    assert(caught.getMessage == message)
    assert(Files.exists(Paths.get(outPath)))
  }

  test("Compare different dataset's format") {
    val refPath = getClass.getResource("/dataSample1.csv").toString
    val newPath = getClass.getResource("/dataSample1.json").toString
    val outPath = s"target/test_output/comparison_job/negative/$timePrefix"

    val args = Array(
      "--ref-format", "csv",
      "--ref-delimiter", ",",
      "--ref-header", "true",
      "--new-format", "json",
      "--new-path", newPath,
      "--ref-path", refPath,
      "--out-path", outPath
    )

    DatasetComparisonJob.main(args)

    assert(Files.exists(Paths.get(outPath,"_METRICS")))
  }

  test("Compare ref and new df of the same format.") {
    val refPath = getClass.getResource("/dataSample1.csv").toString
    val newPath = getClass.getResource("/copyDataSample1.csv").toString
    val outPath = s"target/test_output/comparison_job/negative/$timePrefix"

    val args = Array(
      "--ref-format", "csv",
      "--ref-delimiter", ",",
      "--ref-header", "true",
      "--new-format", "csv",
      "--new-header", "true",
      "--new-path", newPath,
      "--ref-path", refPath,
      "--out-path", outPath
    )

    DatasetComparisonJob.main(args)

    assert(Files.exists(Paths.get(outPath,"_METRICS")))
  }

  test("Compare datasets with wrong schemas by generic way") {
    val refPath = getClass.getResource("/dataSample4.csv").toString
    val newPath = getClass.getResource("/dataSample1.csv").toString
    val outPath = s"target/test_output/comparison_job/negative/$timePrefix"
    val message =
      s"""Expected and actual datasets differ in schemas. Difference is:
         |_c5 cannot be found in both schemas""".stripMargin

    val args = Array(
      "--new-format", "csv",
      "--ref-format", "csv",
      "--delimiter", ",",
      "--new-path", newPath,
      "--ref-path", refPath,
      "--out-path", outPath
    )

    val caught = intercept[SchemasDifferException] {
      DatasetComparisonJob.main(args)
    }

    assert(caught.getMessage == message)
  }

  test("Key based compare of different datasets") {
    val refPath = getClass.getResource("/dataSample1.csv").toString
    val newPath = getClass.getResource("/dataSample3.csv").toString
    val outPath = s"target/test_output/comparison_job/negative/$timePrefix"
    val message = "Expected and actual datasets differ.\n" +
      s"Reference path: $refPath\n" +
      s"Actual dataset path: $newPath\n" +
      s"Difference written to: $outPath\n" +
      "Count Expected( 9 ) vs Actual( 10 )"

    val args = Array(
      "--new-format", "csv",
      "--ref-format", "csv",
      "--delimiter", ",",
      "--header", "true",
      "--new-path", newPath,
      "--ref-path", refPath,
      "--out-path", outPath,
      "--keys", "id"
    )

    val caught = intercept[DatasetsDifferException] {
      DatasetComparisonJob.main(args)
    }

    assert(caught.getMessage == message)
    assert(Files.exists(Paths.get(outPath)))
  }

  test("Compare datasets with duplicates disallowed") {
    val refPath = getClass.getResource("/dataSample1.csv").toString
    val newPath = getClass.getResource("/dataSample5.csv").toString
    val outPath = s"target/test_output/comparison_job/negative/$timePrefix"
    val message = s"""Provided datasets have duplicate rows.
                     |Reference Dataset has 0 duplicates
                     |New Dataset has 1 duplicates""".stripMargin

    val args = Array(
      "--new-format", "csv",
      "--ref-format", "csv",
      "--delimiter", ",",
      "--header", "true",
      "--new-path", newPath,
      "--ref-path", refPath,
      "--out-path", outPath,
      "--keys", "id,first_name"
    )

    val caught = intercept[DuplicateRowsInDF] {
      DatasetComparisonJob.main(args)
    }

    assert(caught.getMessage == message)
  }

  test("Compare nested structures with errors") {
    val expected = Array("WrappedArray(legs_0_conditions_0_checks_0_checkNums_5)", "WrappedArray(legs_0_legid)")
    val refPath = getClass.getResource("/json_orig").toString
    val newPath = getClass.getResource("/json_changed").toString
    val outPath = s"target/test_output/comparison_job/negative/$timePrefix"

    val args = Array(
      "--format", "parquet",
      "--new-path", newPath,
      "--ref-path", refPath,
      "--out-path", outPath,
      "--keys", "id"
    )

    intercept[DatasetsDifferException] {
      DatasetComparisonJob.main(args)
    }
    val df = spark.read.format("parquet").load(outPath)
    val actual: Array[String] = df.select("errCol").collect().flatMap(_.toSeq).map(_.toString)

    assert(actual sameElements expected)
  }

  test("Key based compare of xml files with compound keys") {
    val path: URL = getClass.getResource("/xml_examples/example12_diff.json")
    val expectedDiff: String = FileReader.usingFile(Source.fromURL(path)) { _.mkString.trim }

    val refPath = getClass.getResource("/xml_examples/example1.xml").toString
    val newPath = getClass.getResource("/xml_examples/example2.xml").toString
    val outPath = s"target/test_output/comparison_job/negative/$timePrefix"

    val args = Array(
      "--new-format", "xml",
      "--ref-format", "xml",
      "--rowTag", "row",
      "--new-path", newPath,
      "--ref-path", refPath,
      "--out-path", outPath,
      "--keys", "id,id2,ckey.value"
    )

    intercept[DatasetsDifferException] {
      DatasetComparisonJob.main(args)
    }

    val df = spark.read
      .format("parquet")
      .load(outPath)
      .orderBy("expected_id", "expected_id2", "actual_id", "actual_id2", "actual_value")

    val actualDiff = df.toJSON.collect().mkString("\n")

    assert(actualDiff == expectedDiff)
  }
}
