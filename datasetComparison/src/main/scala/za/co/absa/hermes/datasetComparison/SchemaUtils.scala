/*
 * Copyright 2019 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.hermes.datasetComparison

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame}

object SchemaUtils {
  /**
   * Compares 2 array fields of a dataframe schema.
   *
   * @param array1 The first array to compare
   * @param array2 The second array to compare
   * @return true if provided arrays are the same ignoring nullability
   */
  @scala.annotation.tailrec
  private def isSameArray(array1: ArrayType, array2: ArrayType): Boolean = {
    array1.elementType match {
      case arrayType1: ArrayType =>
        array2.elementType match {
          case arrayType2: ArrayType => isSameArray(arrayType1, arrayType2)
          case _ => false
        }
      case structType1: StructType =>
        array2.elementType match {
          case structType2: StructType => isSameSchema(structType1, structType2)
          case _ => false
        }
      case _ => array1.elementType == array2.elementType
    }

  }

  @scala.annotation.tailrec
  private def diffArray(array1: ArrayType, array2: ArrayType, parent: String): Seq[String] = {
    array1.elementType match {
      case _ if array1.elementType.typeName != array2.elementType.typeName =>
        Seq(s"$parent data type doesn't match (${array1.elementType.typeName}) vs (${array2.elementType.typeName})")
      case arrayType1: ArrayType =>
        diffArray(arrayType1, array2.elementType.asInstanceOf[ArrayType], s"$parent")
      case structType1: StructType =>
        diffSchema(structType1, array2.elementType.asInstanceOf[StructType], s"$parent")
      case _ => Seq.empty[String]
    }

  }

  /**
   * Compares 2 fields of a dataframe schema.
   *
   * @param field1 The first field to compare
   * @param field2 The second field to compare
   * @return true if provided fields are the same ignoring nullability
   */
  private def isSameField(field1: StructField, field2: StructField): Boolean = {
    field1.dataType match {
      case arrayType1: ArrayType =>
        field2.dataType match {
          case arrayType2: ArrayType => isSameArray(arrayType1, arrayType2)
          case _ => false
        }
      case structType1: StructType =>
        field2.dataType match {
          case structType2: StructType => isSameSchema(structType1, structType2)
          case _ => false
        }
      case _ => field1.dataType == field2.dataType
    }
  }

  private def diffField(field1: StructField, field2: StructField, parent: String): Seq[String] = {
    field1.dataType match {
      case _ if field1.dataType.typeName != field2.dataType.typeName =>
        Seq(s"$parent.${field1.name} data type doesn't match (${field1.dataType.typeName}) vs (${field2.dataType.typeName})")
      case arrayType1: ArrayType =>
        diffArray(arrayType1, field2.dataType.asInstanceOf[ArrayType], s"$parent.${field1.name}")
      case structType1: StructType =>
        diffSchema(structType1, field2.dataType.asInstanceOf[StructType], s"$parent.${field1.name}")
      case _ =>
        Seq.empty[String]
    }
  }

  /**
   * Returns data selector that can be used to align schema of a data frame.
   * @param schema Schema that serves as the model of column order
   * @return Sorted DF to conform to schema
   */
  def getDataFrameSelector(schema: StructType): List[Column] = {
    import za.co.absa.spark.hofs._

    def processArray(arrType: ArrayType, column: Column, name: String): Column = {
      arrType.elementType match {
        case arrType: ArrayType =>
          transform(column, x => processArray(arrType, x, name)).as(name)
        case nestedStructType: StructType =>
          transform(column, x => struct(processStruct(nestedStructType, Some(x)): _*)).as(name)
        case _ => column
      }
    }

    def processStruct(curSchema: StructType, parent: Option[Column]): List[Column] = {
      curSchema.foldRight(List.empty[Column])((field, acc) => {
        val currentCol: Column = parent match {
          case Some(x) => x.getField(field.name).as(field.name)
          case None    => col(field.name)
        }
        field.dataType match {
          case arrType: ArrayType     => processArray(arrType, currentCol, field.name) :: acc
          case structType: StructType => struct(processStruct(structType, Some(currentCol)): _*).as(field.name) :: acc
          case _                      =>  currentCol :: acc
        }
      })
    }

    processStruct(schema, None)
  }

  def alignSchema(df: DataFrame, selector: List[Column]): DataFrame = df.select(selector: _*)

  /**
   * Compares 2 dataframe schemas.
   *
   * @param schema1 The first schema to compare
   * @param schema2 The second schema to compare
   * @return true if provided schemas are the same ignoring nullability
   */
  def isSameSchema(schema1: StructType, schema2: StructType): Boolean = {
    val fields1 = schema1.map(field => field.name.toLowerCase() -> field).toMap
    val fields2 = schema2.map(field => field.name.toLowerCase() -> field).toMap

    // Checking if every field in schema1 exists in schema2
    val same1 = fields1.values.foldLeft(true)((stillSame, field1) => {
      val field1NameLc = field1.name.toLowerCase()
      if (fields2.contains(field1NameLc)) {
        val field2 = fields2(field1NameLc)
        stillSame && isSameField(field1, field2)
      } else {
        false
      }
    })

    // Checking if every field in schema2 exists in schema1
    val same2 = fields2.values.foldLeft(true)((stillSame, field2) => {
      val field2NameLc = field2.name.toLowerCase()
      if (fields1.contains(field2NameLc)) {
        val field1 = fields1(field2NameLc)
        stillSame && isSameField(field2, field1)
      } else {
        false
      }
    })
    same1 && same2
  }

  def diffSchema(schema1: StructType, schema2: StructType, parent: String = ""): Seq[String] = {
    val fields1 = schema1.map(field => field.name.toLowerCase() -> field).toMap
    val fields2 = schema2.map(field => field.name.toLowerCase() -> field).toMap

    val diff = fields1.values.foldLeft(Seq.empty[String])((difference, field1) => {
      val field1NameLc = field1.name.toLowerCase()
      if (fields2.contains(field1NameLc)) {
        val field2 = fields2(field1NameLc)
        difference ++ diffField(field1, field2, parent)
      } else {
        difference ++ Seq(s"$parent.${field1.name} cannot be found in both schemas")
      }
    })

    diff.map(_.stripPrefix("."))
  }
}
