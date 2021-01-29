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

case class CliHelpOptions(key: String, optional: String, text: String){
  override def toString: String = f"$key%-26s$optional%-11s$text"
}

case class CliHelp(title: String, example: String, description: String, options: List[CliHelpOptions]) {

  override def toString: String =
    s"""$title
       |$description
       |$example
       |Options:
       |${options.mkString("\n")}""".stripMargin
}
