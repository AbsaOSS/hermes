package za.co.absa.hermes.datasetComparison

import org.scalatest.FunSuite

import scala.util.{Failure, Success}

class ConfigSuite extends FunSuite {

  private val newPath = "/tmp/standardized_out"
  private val refPath = "/tmp/reference_data"
  private val outPath = "/tmp/out_data"
  private val delimiter = ";"
  private val rowTag = "Alfa"
  private val csvFormat = "csv"
  private val parquetFormat = "parquet"
  private val xmlFormat = "xml"

  test("Parquet file") {
    val cmdConfig = DatasetComparisonConfig.getCmdLineArguments(
      Array(
        "--raw-format", parquetFormat,
        "--new-path", newPath,
        "--ref-path", refPath,
        "--out-path", outPath,
        "--keys", "id"
      )
    ) match {
      case Success(value) => value
      case Failure(exception) => fail(exception)
    }

    assert(cmdConfig.rawFormat == parquetFormat)
    assert(cmdConfig.newPath == newPath)
    assert(cmdConfig.refPath == refPath)
    assert(cmdConfig.outPath == outPath)
    assert(cmdConfig.keys.contains(Seq("id")))
  }

  test("Csv with default header") {
    val cmdConfig = DatasetComparisonConfig.getCmdLineArguments(
      Array(
        "--raw-format", csvFormat,
        "--delimiter", delimiter,
        "--new-path", newPath,
        "--ref-path", refPath,
        "--out-path", outPath,
        "--keys", "id,alfa"
      )
    ) match {
      case Success(value) => value
      case Failure(exception) => fail(exception)
    }

    assert(cmdConfig.rawFormat == csvFormat)
    assert(cmdConfig.csvDelimiter == delimiter)
    assert(!cmdConfig.csvHeader)
    assert(cmdConfig.newPath == newPath)
    assert(cmdConfig.refPath == refPath)
    assert(cmdConfig.outPath == outPath)
    assert(cmdConfig.keys == Some(Seq("id","alfa")))
  }

  test("Csv with header") {
    val cmdConfig = DatasetComparisonConfig.getCmdLineArguments(
      Array(
        "--raw-format", csvFormat,
        "--delimiter", ";",
        "--header", "true",
        "--new-path", newPath,
        "--ref-path", refPath,
        "--out-path", outPath
      )
    ) match {
      case Success(value) => value
      case Failure(exception) => fail(exception)
    }

    assert(cmdConfig.rawFormat == csvFormat)
    assert(cmdConfig.csvDelimiter == delimiter)
    assert(cmdConfig.csvHeader)
    assert(cmdConfig.newPath == newPath)
    assert(cmdConfig.refPath == refPath)
    assert(cmdConfig.outPath == outPath)
  }

  test("XML file") {
    val cmdConfig = DatasetComparisonConfig.getCmdLineArguments(
      Array(
        "--raw-format", xmlFormat,
        "--row-tag", rowTag,
        "--new-path", newPath,
        "--ref-path", refPath,
        "--out-path", outPath
      )
    ) match {
      case Success(value) => value
      case Failure(exception) => fail(exception)
    }

    assert(cmdConfig.rawFormat == xmlFormat)
    assert(cmdConfig.rowTag == Option(rowTag))
    assert(cmdConfig.newPath == newPath)
    assert(cmdConfig.refPath == refPath)
    assert(cmdConfig.outPath == outPath)
  }

  test("Missing mandatory options") {
    val cmdConfig = DatasetComparisonConfig.getCmdLineArguments(
      Array(
        "--new-path", newPath,
        "--out-path", outPath
      )
    ) match {
      case Success(_) => fail("DatasetComparisonConfig returned while it should have thrown an error")
      case Failure(_) => succeed
    }
  }
  test("new and ref files") {
    val cmdConfig = DatasetComparisonConfig.getCmdLineArguments(
      Array(
        "--raw-format", parquetFormat,
        "--new-raw-format", xmlFormat,
        "--new-row-tag", rowTag,
        "--new-path", newPath,
        "--ref-raw-format", csvFormat,
        "--ref-delimiter", delimiter,
        "--ref-header", "true",
        "--ref-path", refPath,
        "--out-path", outPath
      )
    ) match {
      case Success(value) => value
      case Failure(exception) => fail(exception)
    }

    assert(cmdConfig.rawFormat == parquetFormat)
    assert(cmdConfig.newRawFormat == xmlFormat)
    assert(cmdConfig.newRowTag == Option(rowTag))
    assert(cmdConfig.refRawFormat == csvFormat)
    assert(cmdConfig.refCsvDelimiter == delimiter)
    assert(cmdConfig.refCsvHeader)
    assert(cmdConfig.newPath == newPath)
    assert(cmdConfig.refPath == refPath)
    assert(cmdConfig.outPath == outPath)
  }
}
