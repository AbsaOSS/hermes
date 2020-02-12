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

import org.scalatest.FunSuite
import za.co.absa.hermes.datasetComparison.SchemaComparison._
import za.co.absa.hermes.utils.HelperFunctions._
import za.co.absa.hermes.utils.SparkTestBase

class SchemaComparisonSuite extends FunSuite with SparkTestBase  {

  val schemaA = """[{"id":1,"legs":[{"legid":100,"conditions":[{"checks":[{"checkNums":["1","2","3b","4","5c","6"]}],"amount":100}]}]}]"""
  val schemaB = """[{"id":1,"legs":[{"legid":100,"conditions":[{"checks":[{"checkNums":["1","2","3b","4","5c","6"]}],"amount":100,"price":10}]}]}]"""

  test("Test the case when schemas are equal") {
    val dfA1 = getDataFrameFromJson(spark, Seq(schemaA))
    val dfA2 = getDataFrameFromJson(spark, Seq(schemaA))

    assert(isSameSchema(dfA1.schema, dfA2.schema))
  }

  test("Test the case when the first schema has an extra field") {
    val dfA = getDataFrameFromJson(spark, Seq(schemaA))
    val dfB = getDataFrameFromJson(spark, Seq(schemaB))

    assert(!isSameSchema(dfA.schema, dfB.schema))
  }

  test("Test the case when the second schema has an extra field") {
    val dfA = getDataFrameFromJson(spark, Seq(schemaA))
    val dfB = getDataFrameFromJson(spark, Seq(schemaB))

    assert(!isSameSchema(dfB.schema, dfA.schema))
  }
}
