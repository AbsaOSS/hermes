/*
 * Copyright 2019 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.hermes.infoFileComparison

import za.co.absa.atum.utils.SerializationUtils

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
    SerializationUtils.asJsonPretty[List[ModelDifference[_]]](modelDifferences)
  }
}
