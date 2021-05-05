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

package org.apache.spark.sql

object AccessShowString {
  /**
   * Exposure of showString method. Compose the string representing rows for show output. Truncate enforced.
   *
   * @param truncate If set to more than 0, truncates strings to `truncate` characters and
   *                   all cells will be aligned right.
   * @param vertical If set to true, prints output rows vertically (one line per column value).
   * @param df DataFrame to get a show string representation
   * @param _numRows Number of rows to show. Defaults to 20
   * @return String representation of Dataframe
   */
  def showString(df: DataFrame,
                 _numRows: Int = 20,
                 vertical: Boolean = false): String = {
    df.showString(_numRows, truncate = 0, vertical = vertical)
  }
}
