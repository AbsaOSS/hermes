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

import java.nio.file.{Files, Paths}
import java.text.SimpleDateFormat
import java.util.Calendar

import org.scalatest.{BeforeAndAfterEach, FunSuite}
import za.co.absa.hermes.e2eRunner.TestDefinition
import za.co.absa.hermes.infoFileComparison.ModelDifference

class InfoFileComparisonPluginTest extends FunSuite with BeforeAndAfterEach {
  private val plugin = new InfoFileComparisonPlugin()

  private val format = new SimpleDateFormat("yyyy_MM_dd-HH_mm_ss")
  private var timePrefix = ""

  private val modelDiffs = List(
    ModelDifference("metadata.informationDate","01-01-2019","01-01-2020"),
    ModelDifference("metadata.additionalInfo.std_cmd_line_args","--menas-credentials-file /menas-credential.properties --dataset-name DeleteMe --dataset-version 115 --report-date 2019-01-01 --report-version 1 --raw-format json","--menas-auth-keytab /creds.keytab --dataset-name DeleteMe --dataset-version 115 --report-date 2019-01-01 --report-version 1 --raw-format json")
  )
  private val shouldPass = false
  private val order = 111
  private val testName = "UnitTest"
  private val result = InfoFileComparisonResult(Array.empty, modelDiffs, order, testName, shouldPass, Map.empty)

  override def beforeEach(): Unit = {
    timePrefix = format.format(Calendar.getInstance().getTime)
  }

  test("Result - write") {
    val outPath = s"target/test_output/e2e/info_comparison_plugin/${timePrefix}.json"
    val resultWithPath = result.copy(additionalInfo = Map("outPath" -> s"file://$outPath"))
    resultWithPath.write(Array.empty)
    assert(Files.exists(Paths.get(outPath)))
  }

  test("Plugin - name") {
    assert(plugin.name == "InfoFileComparison")
  }

  test("Plugin - performAction") {
    val refPath = s"file://${getClass.getResource("/InfoFileComparisonPlugin/info_file_correct.json").getPath}"
    val newPath = s"file://${getClass.getResource("/InfoFileComparisonPlugin/info_file_wrong.json").getPath}"
    val outPath = s"file://target/test_output/info_comparison/negative/$timePrefix"


    val args = Array(
      "--new-path", newPath,
      "--ref-path", refPath,
      "--out-path", outPath
    )
    val td = TestDefinition(testName, 0, "InfoFileComparison", args, None, None)

    val expectedResult = result.copy(arguments = args)

    val actualResult = plugin.performAction(td, order).asInstanceOf[InfoFileComparisonResult]
      .copy(additionalInfo = Map.empty)

    assert(expectedResult == actualResult)
  }
}
