package za.co.absa.hermes.datasetComparison.config

import org.scalatest.FunSuite

class ManualConfigSuite extends FunSuite {
  test("Manual Config Correct") {
    val conf = new ManualConfig("errCol", "_actual", "_expected", false)
    assert(conf.validate().isSuccess)
    assert("errCol" == conf.errorColumnName)
    assert("_actual" == conf.actualPrefix)
    assert("_expected" == conf.expectedPrefix)
    assert(!conf.allowDuplicates)
  }

  test("Manual Config Bad Column Name") {
    val conf = new ManualConfig("errCol", "_actua l", "_expected", false)
    assert(conf.validate().isFailure)
  }
}
