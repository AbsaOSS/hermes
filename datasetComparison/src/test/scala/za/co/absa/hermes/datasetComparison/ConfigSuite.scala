package za.co.absa.hermes.datasetComparison

import org.scalatest.FunSuite

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
    )

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
    )

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
    )

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
    )

    assert(cmdConfig.rawFormat == xmlFormat)
    assert(cmdConfig.rowTag == Option(rowTag))
    assert(cmdConfig.newPath == newPath)
    assert(cmdConfig.refPath == refPath)
    assert(cmdConfig.outPath == outPath)
  }
}

