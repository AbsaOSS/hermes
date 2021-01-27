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

import org.scalatest.FunSuite
import za.co.absa.atum.utils.SparkTestBase
import za.co.absa.hermes.e2eRunner.logging.functions.Scribe
import za.co.absa.hermes.e2eRunner.plugins.BashJobsResult

class E2ERunnerJobSuite extends FunSuite with SparkTestBase {
  val pluginDefinitions: PluginDefinitions = PluginDefinitions()
  test("runTests") {
    val order = 111
    val testName = "Test1"
    val args = Array("echo", """ "alfa" """)
    val td = TestDefinition(testName, order, "BashPlugin", args, None, None)
    implicit val loggingFunctions: Scribe = Scribe(this.getClass)

    val expectedResults = BashJobsResult(args, "alfa\n", 1, testName, true, Map.empty)
    val results = E2ERunnerJob.runTests(TestDefinitions.fromSeq(Seq(td)), pluginDefinitions)
      .head.asInstanceOf[BashJobsResult]
      .copy(additionalInfo = Map.empty)
    assert(expectedResults == results)
  }
}
