package za.co.absa.hermes.datasetComparison

import java.io.ByteArrayOutputStream
import java.net.URL

import org.apache.spark.sql.DataFrameReader
import org.scalatest.FunSuite
import za.co.absa.hermes.utils.{FileReader, SparkTestBase}

import scala.io.Source

class DataFrameReaderFactorySuite extends FunSuite with SparkTestBase {

  private val delimiter = ","
  private val rowTag = "row"
  private val csvFormat = "csv"
  private val parquetFormat = "parquet"
  private val xmlFormat = "xml"

  test("GetDFReaderFromCmdConfig with xml") {
    val outCapture = new ByteArrayOutputStream
    val path: URL = getClass.getResource("/xml_examples/example1.txt")
    val lines: List[String] = FileReader.usingFile(Source.fromURL(path)) { _.getLines().toList }

    val cmd = DatasetComparisonConfig(xmlFormat, rowTag = Option(rowTag))
    val reader: DataFrameReader = DataFrameReaderFactory.getDFReaderFromCmdConfig(cmd)
    val df = reader.load(getClass.getResource("/xml_examples/example1.xml").getPath)

    Console.withOut(outCapture) { df.show(false) }
    val result = new String(outCapture.toByteArray).replace("\r\n", "\n").split("\n").toList
    assert(lines == result)
  }

  test("GetDFReaderFromCmdConfig with csv without header") {
    val outCapture = new ByteArrayOutputStream
    val path: URL = getClass.getResource("/dataSample1_show_header_false.txt")
    val lines: List[String] = FileReader.usingFile(Source.fromURL(path)) { _.getLines().toList }

    val cmd = DatasetComparisonConfig(csvFormat, csvDelimiter = delimiter)
    val reader: DataFrameReader = DataFrameReaderFactory.getDFReaderFromCmdConfig(cmd)
    val df = reader.load(getClass.getResource("/dataSample1.csv").getPath)

    Console.withOut(outCapture) { df.show(false) }
    val result = new String(outCapture.toByteArray).replace("\r\n", "\n").split("\n").toList
    assert(lines == result)
  }

  test("GetDFReaderFromCmdConfig with csv with header") {
    val outCapture = new ByteArrayOutputStream
    val path: URL = getClass.getResource("/dataSample1_show_header_true.txt")
    val lines: List[String] = FileReader.usingFile(Source.fromURL(path)) { _.getLines().toList }

    val cmd = DatasetComparisonConfig(csvFormat, csvDelimiter = delimiter, csvHeader = true)
    val reader: DataFrameReader = DataFrameReaderFactory.getDFReaderFromCmdConfig(cmd)
    val df = reader.load(getClass.getResource("/dataSample1.csv").getPath)

    Console.withOut(outCapture) { df.show(false) }
    val result = new String(outCapture.toByteArray).replace("\r\n", "\n").split("\n").toList
    assert(lines == result)
  }

  test("GetDFReaderFromCmdConfig with parquet") {
    val outCapture = new ByteArrayOutputStream
    val path: URL = getClass.getResource("/json_orig_show.txt")
    val lines: List[String] = FileReader.usingFile(Source.fromURL(path)) { _.getLines().toList }

    val cmd = DatasetComparisonConfig(parquetFormat)
    val reader: DataFrameReader = DataFrameReaderFactory.getDFReaderFromCmdConfig(cmd)
    val df = reader.load(getClass.getResource("/json_orig").getPath)

    Console.withOut(outCapture) { df.orderBy("id").show(false) }
    val result = new String(outCapture.toByteArray).replace("\r\n", "\n").split("\n").toList
    assert(lines == result)
  }
}
