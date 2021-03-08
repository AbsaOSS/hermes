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

import org.apache.spark.sql.{Column, DataFrame}
import spray.json._

object ComparisonResultProtocol extends DefaultJsonProtocol {
  implicit object ComparisonResultFormat extends RootJsonFormat[ComparisonResult] {
    def write(cr: ComparisonResult): JsObject = JsObject(
      "referenceRowCount" -> JsNumber(cr.refRowCount),
      "newRowCount" -> JsNumber(cr.newRowCount),
      "newDuplicateCount" -> JsNumber(cr.newDuplicateCount),
      "refDuplicateCount" -> JsNumber(cr.refDuplicateCount),
      "passed" -> JsBoolean(cr.diffCount == 0),
      "numberOfDifferences" -> JsNumber(cr.diffCount),
      "passedRowsCount" -> JsNumber(cr.passedCount),
      "passedOptions" -> JsString(cr.passedOptions),
      "additionalInfo" -> JsObject(cr.additionalInfo.map(v => v._1 -> JsString(v._2)))
    )

    override def read(value: JsValue): ComparisonResult = {
      val fields = Seq("referenceRowCount", "newRowCount", "newDuplicateCount", "refDuplicateCount",
        "numberOfDifferences", "passedRowsCount", "passedOptions", "additionalInfo")
      value.asJsObject.getFields(fields: _*) match {
        case Seq(JsNumber(refRowCount), JsNumber(newRowCount), JsNumber(newDuplicateCount),
        JsNumber(refDuplicateCount), JsNumber(diffCount), JsNumber(passedCount), JsString(passedOptions),
        JsObject(additionalInfo)) =>
          ComparisonResult(refRowCount.toLong, newRowCount.toLong, refDuplicateCount.toLong, newDuplicateCount.toLong,
            passedCount.toLong, List.empty[Column], None, diffCount.toLong, passedOptions,
            additionalInfo.map( value => value._1 -> value._2.toString()))
        case _ => throw DeserializationException("ComparisonResult expected")
      }
    }
  }
}

/**
 *
 * @param refRowCount Row Count of the reference data
 * @param newRowCount Row Count of the new data
 * @param usedSchemaSelector Selector used to align schemas created from reference data schema
 * @param resultDF Result dataframe, if None, there were no differences between reference and new data
 * @param diffCount Number of differences if there are any
 * @param passedOptions Raw options passed to the job by user. Might be empty if comparison used as a library
 */
case class ComparisonResult(refRowCount: Long,
                            newRowCount: Long,
                            refDuplicateCount: Long,
                            newDuplicateCount: Long,
                            passedCount: Long,
                            usedSchemaSelector: List[Column],
                            resultDF: Option[DataFrame],
                            diffCount: Long = 0,
                            passedOptions: String = "",
                            additionalInfo: Map[String, String] = Map.empty){
  import ComparisonResultProtocol._

  def passed: Boolean = diffCount == 0

  def getJsonMetadata: JsValue = {
    (this: ComparisonResult).toJson
  }

  def getPrettyJson: String = getJsonMetadata.prettyPrint

  def getMetadata: Map[String, Any] = Map[String, Any](
      "referenceRowCount" -> refRowCount,
      "newRowCount" -> newRowCount,
      "newDuplicateCount" -> newDuplicateCount,
      "refDuplicateCount" -> refDuplicateCount,
      "passed" -> passed,
      "numberOfDifferences" -> diffCount,
      "passedRowsCount" -> passedCount,
      "passedOptions" -> passedOptions,
      "additionalInfo" -> additionalInfo
    )
}

