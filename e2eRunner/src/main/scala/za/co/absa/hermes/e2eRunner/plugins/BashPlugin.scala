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

package za.co.absa.hermes.e2eRunner.plugins

import za.co.absa.hermes.e2eRunner.logging.{InfoResultLog, ResultLog}

import scala.sys.process._
import za.co.absa.hermes.e2eRunner.{Plugin, PluginResult, TestDefinition}
import za.co.absa.hermes.utils.HelperFunctions

case class BashJobsResult(arguments: Array[String],
                          returnedValue: String,
                          order: Int,
                          testName: String,
                          passed: Boolean,
                          additionalInfo: Map[String, String]) extends PluginResult {

  /**
   * This method should be used to write the plugin result in a form required.
   *
   * @param writeArgs Arguments provided from the "writeArgs" key from the test definition json
   */
  override def write(writeArgs: Array[String]): Unit = {
    scribe.warn("BashJob plugin does not support write for the result")
  }

  /**
   * Logs the result of the plugin execution at the end.
   */

  override def resultLog: ResultLog = {
    InfoResultLog(s"Test $testName ($order) finished. Bash job finished with a zero exit.")
  }
}

class BashPlugin extends Plugin {

  override def name: String = "BashPlugin"

  override def performAction(testDefinition: TestDefinition, actualOrder: Int): BashJobsResult = {
    def runBashCmd(bashCmd: String): String = {
      (s"echo $bashCmd" #| "bash").!!
    }

    val args = testDefinition.args
    val testName = testDefinition.name

    scribe.info(s"Running bash with: ${args.mkString(" ")}")
    val (confTime, returnValue) = HelperFunctions.calculateTime { runBashCmd(args.mkString(" ")) }
    val additionalInfo = Map("elapsedTimeInMilliseconds" -> confTime.toString)
    BashJobsResult(args, returnValue, actualOrder, testName, passed = true, additionalInfo)
  }
}
