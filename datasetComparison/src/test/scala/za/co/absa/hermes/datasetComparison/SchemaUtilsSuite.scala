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

import org.apache.spark.sql.AnalysisException
import org.scalatest.FunSuite
import za.co.absa.hermes.datasetComparison.SchemaUtils._
import za.co.absa.hermes.utils.HelperFunctions._
import za.co.absa.hermes.utils.SparkTestBase

class SchemaUtilsSuite extends FunSuite with SparkTestBase  {

  val schemaA = """[{"id":1,"legs":[{"legid":100,"conditions":[{"checks":[{"checkNums":["1","2","3b","4","5c","6"]}],"amount":100}]}], "key" : {"alfa": "1", "beta": {"beta2": "2"}} }]"""
  val schemaB = """[{"id":1"legs":[{"legid":100,"conditions":[{"checks":[{"checkNums":["1","2","3b","4","5c","6"]}],"amount":100,"price":10}]}]}]"""
  val schemaC = """[{"legs":[{"legid":100,"conditions":[{"amount":100,"checks":[{"checkNums":["1","2","3b","4","5c","6"]}]}]}],"id":1, "key" : {"beta": {"beta2": "2"}, "alfa": "1"} }]"""

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

  test("Test aligning of schemas") {
    val dfA = getDataFrameFromJson(spark, Seq(schemaA))
    val dfC = getDataFrameFromJson(spark, Seq(schemaC)).select("legs", "id", "key")

    val dfA2Aligned = alignSchemas(dfC, getDataframeSelector(dfA.schema))

    assert(dfA.columns.toSeq.equals(dfA2Aligned.columns.toSeq))
    assert(dfA.select("key").columns.toSeq.equals(dfA2Aligned.select("key").columns.toSeq))
  }

  test("Test aligning of different schemas") {
    val dfA = getDataFrameFromJson(spark, Seq(schemaA))
    val dfB = getDataFrameFromJson(spark, Seq(schemaB))

    intercept[AnalysisException] {
      alignSchemas(dfA, getDataframeSelector(dfB.schema))
    }
  }
}
