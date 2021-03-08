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

package za.co.absa.hermes.e2eRunner

import spray.json.{DefaultJsonProtocol, RootJsonFormat}

import scala.util.matching.Regex

object TestDefinitionJsonProtocol extends DefaultJsonProtocol {
  implicit val pluginDefinitionFormat: RootJsonFormat[TestDefinition] = jsonFormat6(TestDefinition.apply)

  // Key for object holding the variables in the test definition json
  val VarsField = "vars"
  // Key for object holding the test runs in the test definition json
  val RunsField: String = "runs"
  // Regex for validation and extractions of vars in the runs object in the test definition json
  val VarsPattern: Regex = """(?<=#\{)[\w_]*(?=\}#)""".r

}
