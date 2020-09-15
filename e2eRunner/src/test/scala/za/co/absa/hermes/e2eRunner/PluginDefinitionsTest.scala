package za.co.absa.hermes.e2eRunner

import java.io.File
import java.nio.file.Paths

import org.scalatest.FunSuite
import za.co.absa.hermes.e2eRunner.plugins._

import scala.reflect.io.Path

class PluginDefinitionsTest extends FunSuite {

  private val pathBackToPlugins = Paths.get(this.getClass.getProtectionDomain.getCodeSource.getLocation.getFile, "..", "..", "..")
  private val classPath = Seq(new File(pathBackToPlugins.toUri))
  private val pluginDefinitions = PluginDefinitions(classPath)

  test("testGetPluginNames") {
    val plugins = pluginDefinitions.getPluginNames
    val expectedPlugins = Set("EnceladusSparkJobs", "InfoFileComparison", "DatasetComparison")
    assert((expectedPlugins -- plugins).isEmpty)
  }

  test("testGetPlugin") {
    val plugin = pluginDefinitions.getPlugin("EnceladusSparkJobs")
    assert(plugin.name == "EnceladusSparkJobs")
    assert(plugin.isInstanceOf[EnceladusSparkJobsPlugin])
  }

}
