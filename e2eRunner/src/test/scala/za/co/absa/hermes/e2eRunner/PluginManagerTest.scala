package za.co.absa.hermes.e2eRunner

import org.scalatest.FunSuite

class PluginManagerTest extends FunSuite {

  test("testGetPlugin") {
    val a = PluginManager()
    val b = Array("a")
    val c = a.getPlugin("DatasetComparison")
    c.performAction(b, 1)
  }

}
