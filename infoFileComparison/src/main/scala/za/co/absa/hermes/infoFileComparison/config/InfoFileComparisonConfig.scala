package za.co.absa.hermes.infoFileComparison.config

abstract class InfoFileComparisonConfig {
  val versionMetaKeys: List[String]
  val keysToIgnore: List[String]
}
