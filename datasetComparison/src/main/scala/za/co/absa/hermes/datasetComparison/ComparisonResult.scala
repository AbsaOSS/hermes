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

import net.liftweb.json.JsonDSL._
import org.apache.spark.sql.{Column, DataFrame}

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
                            passedOptions: String = ""){
  def getJsonMetadata: String = {
    import net.liftweb.json._
    prettyRender(getMetadata)
  }

  def getMetadata: Map[String, String] = Map[String, String](
      "referenceRowCount" -> refRowCount.toString,
      "newRowCount" -> newRowCount.toString,
      "newDuplicateCount" -> newDuplicateCount.toString,
      "refDuplicateCount" -> refDuplicateCount.toString,
      "passed" -> (diffCount == 0).toString,
      "numberOfDifferences" -> diffCount.toString,
      "passedRowsCount" -> passedCount.toString,
      "passedOptions" -> passedOptions
    )
}

