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

import java.io.ByteArrayOutputStream

import org.scalatest.FunSuite
import za.co.absa.hermes.datasetComparison.MissingArgumentException

class CliOptionsSuite extends FunSuite {
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
      "--new-format", "jdbc",
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

    val outDataframeOptions = DataframeOptions(
      "parquet",
      Map.empty[String, String],
      "/some/out/path"
    )

    val cliOptions = CliOptions(
      refDataframeOptions,
      newDataframeOptions,
      outDataframeOptions,
      Set("alfa", "beta"),
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

    assert("""DB table name is mandatory option for format type jdbc. Use "--dbtable" or "--out-dbtable"""" == caught.getMessage)
  }

  test("Test no dbtable for jdbc") {
    val args = Array(
      "--ref-format", "specialFormat",
      "--format", "jdbc",
      "--ref-path", "ref/path/alfa",
      "--out-path", "/some/path"
    )
    val message = """DB table name is mandatory option for format type jdbc. Use "--dbtable" or "--new-dbtable""""

    val caught = intercept[MissingArgumentException] {
      CliOptions.parse(args)
    }

    assert(message == caught.getMessage)
  }
}
