package za.co.absa.hermes.e2eRunner.plugins

import org.scalatest.FunSuite

// IMPORTANT this plugin is currently only a shell based runner of Enceladus, because Enceladus does not support
// library like usage.
class EnceladusSparkJobsPluginTest extends FunSuite {
  private val plugin = new EnceladusSparkJobsPlugin()

  test("Plugin - name") {
    assert(plugin.name == "EnceladusSparkJobs")
  }

  test("Plugin - performAction") {
    val shouldPass = true
    val order = 111
    val testName = "UnitTest"
    val args = Array("echo", "Something Something")

    val expectedResult = EnceladusSparkJobsResult(args, "Something Something\n", order, testName, shouldPass, Map.empty)

    val result = plugin.performAction(args, order, testName).asInstanceOf[EnceladusSparkJobsResult]
      .copy(additionalInfo = Map.empty)

    assert(expectedResult == result)
  }
}
