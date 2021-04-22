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

package za.co.absa.hermes.datasetComparison.cliUtils

import org.scalatest.FunSuite
import za.co.absa.hermes.datasetComparison.MissingArgumentException
import za.co.absa.hermes.datasetComparison.dataFrame.{Parameters, Utils}
import za.co.absa.hermes.utils.SparkTestBase

class ParametersSuite extends FunSuite with SparkTestBase {
  test("Test successful parse with path") {
    val map = Map(
      "format" -> "alfa",
      "option-one" -> "option-one-value",
      "option-two" -> "option-two-value",
      "path" -> "some/path"
    )

    val dfOptoons = Parameters(
      "alfa",
      Map("option-one" -> "option-one-value", "option-two" -> "option-two-value"),
      "some/path"
    )

    assert(dfOptoons == Parameters.validateAndCreate(map))
  }

  test("Test successful parse with jdbc") {
    val map = Map("format" -> "jdbc", "dbtable" -> "table1")

    val dfOptoons = Parameters(
      "jdbc",
      Map("dbtable" -> "table1"),
      "table1"
    )

    assert(dfOptoons == Parameters.validateAndCreate(map))
  }

  test("Test unsuccessful parse missing format") {
    val map = Map("dbtable" -> "table1")

    val caught = intercept[MissingArgumentException] {
      Parameters.validateAndCreate(map)
    }

    assert("""Format is mandatory option. Use "--format"""" == caught.message)
  }

  test("Test unsuccessful parse missing path") {
    val map = Map("format" -> "xml")

    val caught = intercept[MissingArgumentException] {
      Parameters.validateAndCreate(map)
    }

    assert("""Path is mandatory option for all format types except jdbc. Use "--path"""" == caught.message)
  }

  test("Test unsuccessful parse missing dbtable") {
    val map = Map("format" -> "jdbc")

    val caught = intercept[MissingArgumentException] {
      Parameters.validateAndCreate(map)
    }

    assert("""DB table name is mandatory option for format type jdbc. Use "--dbtable"""" == caught.message)
  }

  test("Test load of dataframe") {
    val dfOptions = Parameters(
      "csv",
      Map("header" -> "true", "delimiter" -> ","),
      getClass.getResource("/dataSample1.csv").toString
    )

    val df = Utils.loadDataFrame(dfOptions)
    assert(9L == df.count())
    assert("id first_name last_name email gender ip_address" == df.columns.mkString(" "))
  }
}
