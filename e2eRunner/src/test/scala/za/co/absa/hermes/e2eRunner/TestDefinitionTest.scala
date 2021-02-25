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

import scala.util.{Failure, Success, Try}

class TestDefinitionTest extends FunSuite {

  private val testDefinitions: TestDefinitions =
    TestDefinitions.fromFile(getClass.getResource("/TestDefinitionBase.json").getFile)
  private val badTestDefinitions: TestDefinitions =
    TestDefinitions.fromFile(getClass.getResource("/TestDefinitionBadDependency.json").getFile)
  private val testDefinitionsNoVars: TestDefinitions =
    TestDefinitions.fromFile(getClass.getResource("/TestDefinitionNoVars.json").getFile)
  private val testDefinitionsWithExtras: TestDefinitions = TestDefinitions.fromFile(
    getClass.getResource("/TestDefinitionBase.json").getFile,
    Map("prefix" -> "SomethingOther")
  )
  private val testDefinitionsNonSpecifiedVars: TestDefinitions = TestDefinitions.fromFile(
    getClass.getResource("/TestDefinitionNonSpecifiedVars.json").getFile,
    Map("prefix" -> "SomethingOther")
  )

  test("size") {
    assert(3 == testDefinitions.size)
  }

  test("plugin names") {
    assert(Set("BashPlugin", "InfoComparison", "DatasetComparison") == testDefinitions.getPluginNames)
  }

  test("getSorted") {
    val sortedDefs = testDefinitions.getSorted.map(x => (x.name, x.pluginName))
    assert(List(("Test1","BashPlugin"), ("Test3","DatasetComparison"), ("Test2","InfoComparison")) == sortedDefs)
  }

  test("getSortedWithIndex") {
    val sortedDefs = testDefinitions.getSortedWithIndex.map(x => (x.definition.name, x.definition.pluginName, x.actualOrder))
    val list = List(
      ("Test1","BashPlugin", 1),
      ("Test3","DatasetComparison", 2),
      ("Test2","InfoComparison", 3)
    )
    assert(list == sortedDefs)
  }

  test("ensureOrderAndDependenciesCorrect") {
    Try { testDefinitions.ensureOrderAndDependenciesCorrect() } match {
      case Success(_) => succeed
      case Failure(value) if !value.isInstanceOf[TestDefinitionDependenciesOutOfOrder] =>
        fail(s"ensureOrderAndDependenciesCorrect threw unexpected error: $value")
    }
  }

  test("ensureOrderAndDependenciesCorrect with bad input") {
    val expectedCaughtDeps = Seq("Test1")
    val TestDefinitionDependenciesOutOfOrder(caught) = intercept[TestDefinitionDependenciesOutOfOrder] {
      badTestDefinitions.ensureOrderAndDependenciesCorrect()
    }

    assert(expectedCaughtDeps == caught)
  }

  test("testDefinitionBase") {
    val defsSorted: Seq[TestDefinition] = testDefinitions.getSorted
    val bashPluginDef = defsSorted(0)
    val datasetComparisonJob = defsSorted(1)
    val infoFileComparisonJob = defsSorted(2)

    val expectedBashPluginDef = TestDefinition("Test1", 0, "BashPlugin", Array("Some Random PreFIX stuff", "-a", "b"), None, None)
    val expectedDatasetComparisonJob = TestDefinition("Test3", 1, "DatasetComparison", Array("Some Random PreFIX stuff", "nothing", "extra"),
      Some("Test1"), Some(Array("some", "args", "for", "output")))
    val expectedInfoFileComparisonJob = TestDefinition("Test2", 1, "InfoComparison", Array("info", "file"), Some("Test1"), Some(Array.empty))

    assert(expectedBashPluginDef.equals(bashPluginDef))
    assert(expectedDatasetComparisonJob.equals(datasetComparisonJob))
    assert(expectedInfoFileComparisonJob.equals(infoFileComparisonJob))
  }

  test("extraVars overwrite") {
    val defsSorted: Seq[TestDefinition] = testDefinitionsWithExtras.getSorted
    assert("SomethingOther".equals(defsSorted.head.args.head))
    assert("SomethingOther".equals(defsSorted(1).args.head))
  }

  test("testDefinitionNoVars") {
    val defsSorted: Seq[TestDefinition] = testDefinitionsNoVars.getSorted
    val bashPluginDef = defsSorted(0)
    val datasetComparisonJob = defsSorted(1)
    val infoFileComparisonJob = defsSorted(2)

    val expectedBashPluginDef = TestDefinition("Test1", 0, "BashPlugin", Array("RunThis", "-a", "b"), None, None)
    val expectedDatasetComparisonJob = TestDefinition("Test3", 1, "DatasetComparison", Array("RunThis", "nothing", "extra"),
      Some("Test1"), Some(Array("some", "args", "for", "output")))
    val expectedInfoFileComparisonJob = TestDefinition("Test2", 1, "InfoComparison", Array("info", "file"), Some("Test1"), Some(Array.empty))

    assert(expectedBashPluginDef.equals(bashPluginDef))
    assert(expectedDatasetComparisonJob.equals(datasetComparisonJob))
    assert(expectedInfoFileComparisonJob.equals(infoFileComparisonJob))
  }

  test("extra vars fills non specified vars") {
    val defsSorted: Seq[TestDefinition] = testDefinitionsNonSpecifiedVars.getSorted
    assert("SomethingOther".equals(defsSorted.head.args.head))
    assert("SomethingOther".equals(defsSorted(1).args.head))
  }

  test("Non applied vars throw error") {
    val unusedVars = Set("prefix", "alfa", "")
    val UndefinedVariablesInTestDefinitionJson(errorSet) = intercept[UndefinedVariablesInTestDefinitionJson] {
      TestDefinitions.fromFile(getClass.getResource("/TestDefinitionNonSpecifiedVarsBad.json").getFile)
    }

    assert(unusedVars == errorSet)
  }
}
