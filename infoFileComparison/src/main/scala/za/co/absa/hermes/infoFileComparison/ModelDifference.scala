package za.co.absa.hermes.infoFileComparison

import za.co.absa.atum.utils.ControlUtils

/**
  * A case class for the differences and the JSONPath like path to them
  *
  * @param path Path to the difference through the model
  * @param was  Value it had in reference
  * @param is   Value it has now
  * @tparam T   Type of values
  */
case class ModelDifference[T](path: String, was: T, is: T)

/**
  * This object is used for [[ModelDifference]] object serialization
  */
object ModelDifferenceParser {
  /**
    * The method returns JSON representation of a [[ModelDifference]] object
    */
  def asJson(modelDifferences: List[ModelDifference[_]]): String = {
    ControlUtils.asJsonPretty[List[ModelDifference[_]]](modelDifferences)
  }
}
