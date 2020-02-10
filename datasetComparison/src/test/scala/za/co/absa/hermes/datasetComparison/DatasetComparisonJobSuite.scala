package za.co.absa.hermes.datasetComparison

import java.io.ByteArrayOutputStream
import java.net.URL
import java.nio.file.{Files, Paths}
import java.text.SimpleDateFormat
import java.util.Calendar

import org.scalatest.{BeforeAndAfterEach, FunSuite}
import za.co.absa.hermes.utils.FileReader
import za.co.absa.hermes.utils.SparkTestBase

import scala.io.Source

class DatasetComparisonJobSuite extends FunSuite with SparkTestBase with BeforeAndAfterEach {
  val format = new SimpleDateFormat("yyyy_MM_dd-HH_mm_ss")
  var timePrefix = ""

  override def beforeEach(): Unit = {
    timePrefix = format.format(Calendar.getInstance().getTime)
  }

  test("Compare the same datasets by generic way") {
    val outPath = s"target/test_output/comparison_job/positive/$timePrefix"

    val args = Array(
      "--format", "csv",
      "--delimiter", ",",
      "--new-path", getClass.getResource("/dataSample1.csv").toString,
      "--ref-path", getClass.getResource("/dataSample2.csv").toString,
      "--outPath", outPath
    )
    DatasetComparisonJob.main(args)


    assert(!Files.exists(Paths.get(outPath)))
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
      "--format", "csv",
      "--delimiter", ",",
      "--new-path", newPath,
      "--ref-path", refPath,
      "--outPath", outPath
    )

    val caught = intercept[DatasetsDifferException] {
      DatasetComparisonJob.main(args)
    }

    assert(caught.getMessage == message)
    assert(Files.exists(Paths.get(outPath)))
    assert(2 == Files.list(Paths.get(outPath)).count())
  }

//   TODO issue #24


//  test("Compare different dataset's format") {
//    val refPath = getClass.getResource("/dataSample1.csv").toString
//    val newPath = getClass.getResource("/dataSample1.json").toString
//    val outPath = s"target/test_output/comparison_job/negative/$timePrefix"
//
//    val args = Array(
//      "--ref-raw-format", "csv",
//      "--ref-delimiter", ",",
//      "--ref-header", "true",
//      "--new-raw-format", "json",
//      "--new-path", newPath,
//      "--ref-path", refPath,
//      "--out-path", outPath
//    )
//
//    DatasetComparisonJob.main(args)
//
//
//    assert(!Files.exists(Paths.get(outPath)))
//  }

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
      "--outPath", outPath
    )

    DatasetComparisonJob.main(args)


    assert(!Files.exists(Paths.get(outPath)))
  }

  test("Compare datasets with wrong schemas by generic way") {
    val refPath = getClass.getResource("/dataSample4.csv").toString
    val newPath = getClass.getResource("/dataSample1.csv").toString
    val outPath = s"target/test_output/comparison_job/negative/$timePrefix"
    val diff = "List(StructField(_c5,StringType,true))"
    val message = "Expected and actual datasets differ in schemas.\n" +
      s"Reference path: $refPath\n" +
      s"Actual dataset path: $newPath\n" +
      s"Difference is $diff"

    val args = Array(
      "--format", "csv",
      "--delimiter", ",",
      "--new-path", newPath,
      "--ref-path", refPath,
      "--outPath", outPath
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
      "--format", "csv",
      "--delimiter", ",",
      "--header", "true",
      "--new-path", newPath,
      "--ref-path", refPath,
      "--outPath", outPath,
      "--keys", "id"
    )

    val caught = intercept[DatasetsDifferException] {
      DatasetComparisonJob.main(args)
    }

    assert(caught.getMessage == message)
    assert(Files.exists(Paths.get(outPath)))
  }

  test("Compare datasets with duplicates") {
    val refPath = getClass.getResource("/dataSample1.csv").toString
    val newPath = getClass.getResource("/dataSample5.csv").toString
    val outPath = s"target/test_output/comparison_job/negative/$timePrefix"
    val message = s"Provided dataset has duplicate rows. Specific rows written to $outPath"

    val args = Array(
      "--format", "csv",
      "--delimiter", ",",
      "--header", "true",
      "--new-path", newPath,
      "--ref-path", refPath,
      "--outPath", outPath,
      "--keys", "id,first_name"
    )

    val caught = intercept[DuplicateRowsInDF] {
      DatasetComparisonJob.main(args)
    }

    assert(caught.getMessage == message)
    assert(Files.exists(Paths.get(outPath)))
  }

  test("Compare nested structures with errors") {
    val path: URL = getClass.getResource("/json_output.txt")
    val lines: List[String] = FileReader.usingFile(Source.fromURL(path)) { _.getLines().toList }
    val outCapture = new ByteArrayOutputStream

    val refPath = getClass.getResource("/json_orig").toString
    val newPath = getClass.getResource("/json_changed").toString
    val outPath = s"target/test_output/comparison_job/negative/$timePrefix"

    val args = Array(
      "--format", "parquet",
      "--new-path", newPath,
      "--ref-path", refPath,
      "--outPath", outPath,
      "--keys", "id"
    )

    intercept[DatasetsDifferException] {
      DatasetComparisonJob.main(args)
    }
    val df = spark.read.format("parquet").load(outPath)

    Console.withOut(outCapture) { df.show(false) }
    val result = new String(outCapture.toByteArray).replace("\r\n", "\n").split("\n").toList

    assert(lines == result)
  }

  test("Key based compare of xml files with compound keys") {
    val path: URL = getClass.getResource("/xml_examples/example12_diff.json")
    val expectedDiff: String = FileReader.usingFile(Source.fromURL(path)) { _.mkString.trim }

    val refPath = getClass.getResource("/xml_examples/example1.xml").toString
    val newPath = getClass.getResource("/xml_examples/example2.xml").toString
    val outPath = s"target/test_output/comparison_job/negative/$timePrefix"

    val args = Array(
      "--format", "xml",
      "--rowTag", "row",
      "--new-path", newPath,
      "--ref-path", refPath,
      "--outPath", outPath,
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
