package za.co.absa.hermes.infoFileComparison.config

import com.typesafe.config.{Config, ConfigFactory}

class TypesafeConfig(path: Option[String]) extends InfoFileComparisonConfig {
  private val conf: Config = path match {
    case Some(x) => ConfigFactory.load(x)
    case None    => ConfigFactory.load()
  }

  import collection.JavaConversions._

  val versionMetaKeys: List[String] = conf.getStringList("info-file-comparison.atum-models.versionMetaKeys").toList
  val keysToIgnore: List[String] = conf.getStringList("info-file-comparison.atum-models.ignoredMetaKeys").toList
}
