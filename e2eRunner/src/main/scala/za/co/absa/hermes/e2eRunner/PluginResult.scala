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

import za.co.absa.hermes.e2eRunner.logging.ResultLog

trait PluginResult {

  /**
   * Passed Arguments to the Plugin implementation
   */
  val arguments: Array[String]
  /**
   * Value Returned by Plugin implementation
   */
  val returnedValue: Any
  /**
   * Actual order of the plugin that was ran
   */
  val order: Int
  /**
   * Name of the test
   */
  val testName: String
  /**
   * State of the test. Passed or failed
   */
  val passed: Boolean
  /**
   * Additional info passed to the result by the plugin implementation
   */
  val additionalInfo: Map[String, String]

  /**
   * This method should be used to write the plugin result in a form required.
   *
   * @param writeArgs Arguments provided from the "writeArgs" key from the test definition json
   */
  def write(writeArgs: Array[String]): Unit = {
    throw new NotImplementedError(s"PluginResult ${this.getClass} does not have an implementation of write function")
  }

  /**
   * @return Returns true or false depending if the plugin/test execution passed or failed
   */
  def testPassed: Boolean = this.passed

  /**
   * Implement this method to be able to log the result of the plugin execution at the end.
   */
  def resultLog: ResultLog

  /**
   * @return Returns a name of the test that was executed and which result this instance represents
   */
  def getTestName: String = testName
}
