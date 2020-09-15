package za.co.absa.hermes.e2eRunner

import java.net.URL

import org.scalatest.FunSuite

class TestDefinitionTest extends FunSuite {

  private val testDefinitions: TestDefinitions = TestDefinitions.fromFile(getClass.getResource("/TestDefinitionBase.json").getFile)

  test("size") {
    assert(3 == testDefinitions.size)
  }

  test("plugin names") {
    assert(Set("EnceladusSparkJobs", "InfoComparison", "DatasetComparison") == testDefinitions.getPluginNames)
  }

  test("getSorted") {
    val sortedDefs = testDefinitions.getSorted.map(x => (x.name, x.pluginName))
    assert(List(("Test1","EnceladusSparkJobs"), ("Test3","DatasetComparison"), ("Test2","InfoComparison")) == sortedDefs)
  }

  test("getSortedWithIndex") {
    val sortedDefs = testDefinitions.getSortedWithIndex.map(x => (x.definition.name, x.definition.pluginName, x.actualOrder))
    val list = List(
      ("Test1","EnceladusSparkJobs", 1),
      ("Test3","DatasetComparison", 2),
      ("Test2","InfoComparison", 3)
    )
    assert(list == sortedDefs)
  }

  test("testDefinitionBase") {
    val defsSorted: Seq[TestDefinition] = testDefinitions.getSorted
    val enceladusSparkJob = defsSorted(0)
    val datasetComparisonJob = defsSorted(1)
    val infoFileComparisonJob = defsSorted(2)

    val expectedEnceladusSparkJob = TestDefinition("Test1", 0, "EnceladusSparkJobs", Array("Some Random PreFIX stuff", "-a", "b"), None, None)
    val expectedDatasetComparisonJob = TestDefinition("Test3", 1, "DatasetComparison", Array("Some Random PreFIX stuff", "nothing", "extra"),
      Some("Test1"), Some(Array("some", "args", "for", "output")))
    val expectedInfoFileComparisonJob = TestDefinition("Test2", 1, "InfoComparison", Array("info", "file"), Some("Test1"), Some(Array.empty))

    assert(expectedEnceladusSparkJob.equals(enceladusSparkJob))
    assert(expectedDatasetComparisonJob.equals(datasetComparisonJob))
    assert(expectedInfoFileComparisonJob.equals(infoFileComparisonJob))

  }
}
