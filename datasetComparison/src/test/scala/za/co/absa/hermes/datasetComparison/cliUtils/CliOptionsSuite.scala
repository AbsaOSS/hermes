package za.co.absa.hermes.datasetComparison.cliUtils

import java.io.ByteArrayOutputStream

import org.apache.commons.cli.MissingArgumentException
import org.scalatest.FunSuite

class CliOptionsSuite extends FunSuite{
  test("Test generate help") {
    val outCapture = new ByteArrayOutputStream
    val expectedResult = """Dataset Comparison Tool
                           |Dataset comparison is a command line tool for comparison of two different data sets
                           |spark-submit datasetComparison.jar [OPTIONS]
                           |Options:
                           |--[ref|new]-format        mandatory  Format of the sources
                           |--outPath                 mandatory  Path where will the difference output will be writen to
                           |--new-path|--new-dbtable  mandatory  Path to the newly created source or name of the table
                           |--ref-path|--ref-dbtable  mandatory  Path to the referential source or name of the table
                           |--keys                    optional   Unique columns that will be used as an anchor for data comparison. Without them, the comparison cannot give paths to differences
                           |others                    optional   Options like delimiter, header, rowTag, user, password, url, ... These are the specific options for specific formats used. For more information, check sparks documentation on what all the options for the format you are using
                           |""".stripMargin
    Console.withOut(outCapture) { CliOptions.generateHelp }

    assert(expectedResult == outCapture.toString())
  }

  test("Test a successful parse") {
    val args = Array(
      "--ref-format", "specialFormat",
      "--format", "jdbc",
      "--ref-delimiter", ";",
      "--new-something", "this",
      "--new-else", "that",
      "--new-dbtable", "table1",
      "--out-path", "/some/out/path",
      "--ref-path", "ref/path/alfa",
      "--keys", "alfa,beta"
    )

    val refDataframeOptions = DataframeOptions(
      "specialFormat",
      Map("delimiter" -> ";"),
      "ref/path/alfa"
    )
    val newDataframeOptions = DataframeOptions(
      "jdbc",
      Map("something" -> "this", "else" -> "that", "dbtable" -> "table1"),
      "table1"
    )

    val cliOptions = CliOptions(
      refDataframeOptions,
      newDataframeOptions,
      "/some/out/path",
      Some(Set("alfa", "beta")),
      args.mkString(" "))

    assert(cliOptions == CliOptions.parse(args))
  }

  test("Test missing out path") {
    val args = Array(
      "--ref-format", "specialFormat",
      "--format", "jdbc",
      "--new-dbtable", "table1",
      "--ref-path", "ref/path/alfa"
    )

    val caught = intercept[MissingArgumentException] {
      CliOptions.parse(args)
    }

    assert("""out-path is mandatory option. Use "--out-path".""" == caught.getMessage)
  }

  test("Test wrong") {
    val args = Array(
      "--ref-format", "specialFormat",
      "--format", "jdbc",
      "--new-dbtable", "table1",
      "--ref-path", "ref/path/alfa"
    )

    val caught = intercept[MissingArgumentException] {
      CliOptions.parse(args)
    }

    assert("""out-path is mandatory option. Use "--out-path".""" == caught.getMessage)
  }
}
