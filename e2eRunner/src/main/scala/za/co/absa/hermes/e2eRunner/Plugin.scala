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

package za.co.absa.hermes.e2eRunner

abstract class PluginResult(arguments: Array[String],
                            returnedValue: Any,
                            order: Int,
                            passed: Boolean,
                            additionalInfo: Map[String, String]) {

  /**
   * This method should be used to write the plugin result in a form required.
   *
   * @param writeArgs Arguments provided from the "writeArgs" key from the test definition json
   */
  def write(writeArgs: Seq[String]): Unit = {
    throw new NotImplementedError(s"PluginResult ${this.getClass} does not have an implementation of write function")
  }

  /**
   * @return Returns true or false depending if the plugin/test execution passed or failed
   */
  def testPassed: Boolean = this.passed

  /**
   * Implement this method to log the result of the plugin execution at the end.
   */
  def logResult(): Unit
}

trait Plugin {
  /**
   * Plugin names is here to provide user-friendly name for each plugin
   *
   * @return User-friendly name of the plugin
   */
  def name: String

  /**
   * Perform action is the core method. Executes the plugin and returns a result as a subclass of PluginResult
   *
   * @param args Args similar to main args.
   * @param actualOrder When specifying the order in the test definition json, the number might not be the
   *                    same as the execution number. This number is automatically provided by the PluginManager.
   *                    PluginResult expects this number.
   * @return Returns a subclass of PluginResult.
   */
  def performAction(args: Array[String], actualOrder: Int): PluginResult
}
