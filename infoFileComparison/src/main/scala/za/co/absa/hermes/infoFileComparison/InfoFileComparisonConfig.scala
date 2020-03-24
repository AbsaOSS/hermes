package za.co.absa.hermes.infoFileComparison

import com.typesafe.config.{Config, ConfigFactory}

case class InfoFileComparisonConfig(versionMetaKeys: List[String], keysToIgnore: List[String])

object InfoFileComparisonConfig {
  def fromTypesafeConfig(path: Option[String] = None): InfoFileComparisonConfig = {
    val conf: Config = path match {
      case Some(x) => ConfigFactory.load(x)
      case None    => ConfigFactory.load()
    }

    import collection.JavaConversions._

    val versionMetaKeys = conf.getStringList("info-file-comparison.atum-models.versionMetaKeys").toList
    val keysToIgnore = conf.getStringList("info-file-comparison.atum-models.ignoredMetaKeys").toList
    InfoFileComparisonConfig(versionMetaKeys, keysToIgnore)
  }

  def empty: InfoFileComparisonConfig = InfoFileComparisonConfig(List.empty[String], List.empty[String])
}
