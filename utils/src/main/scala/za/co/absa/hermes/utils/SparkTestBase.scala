package za.co.absa.hermes.utils

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession

trait SparkTestBase { self =>
  val config: Config = ConfigFactory.load()
  val sparkMaster: String = config.getString("utils.sparkTestBaseMaster")

  implicit val spark: SparkSession = SparkSession.builder()
    .master(sparkMaster)
    .appName(s"Hermes unit test - ${self.getClass.getName}")
    .config("spark.ui.enabled", "false")
    .config("spark.debug.maxToStringFields", 100)
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .getOrCreate()
}
