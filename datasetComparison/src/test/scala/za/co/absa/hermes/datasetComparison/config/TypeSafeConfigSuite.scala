package za.co.absa.hermes.datasetComparison.config

import org.scalatest.FunSuite

class TypeSafeConfigSuite extends FunSuite {
  test("Default Config Loaded") {
    val conf = new TypesafeConfig(None)
    assert("errCol" == conf.errorColumnName)
    assert("tmp" == conf.tmpColumnName)
    assert("ComparisonUniqueId" == conf.comparisonUniqueId)
    assert("actual" == conf.actualPrefix)
    assert("expected" == conf.expectedPrefix)
    assert(!conf.allowDuplicates)
    assert(conf.deduplicate)
  }

  test("Config with provided path loaded") {
    val conf = new TypesafeConfig(Some("confData/application.conf"))

    assert("errCol2" == conf.errorColumnName)
    assert("tmp2" == conf.tmpColumnName)
    assert("ComparisonUniqueId2" == conf.comparisonUniqueId)
    assert("actual2" == conf.actualPrefix)
    assert("expected2" == conf.expectedPrefix)
    assert(conf.allowDuplicates)
    assert(!conf.deduplicate)
  }
}
