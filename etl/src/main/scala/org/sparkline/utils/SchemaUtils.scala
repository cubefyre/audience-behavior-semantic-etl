/**
 * SparklineData, Inc. -- http://www.sparklinedata.com/
 *
 * Scala based Audience Behavior APIs
 *
 * Copyright 2014-2015 SparklineData, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 /**
 * Created by Harish.
 */
 package org.sparkline.utils

import org.apache.spark.sql.catalyst.analysis.{UnresolvedExtractValue, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.{Literal, Expression}
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}

object SchemaUtils {

  def field(typ : StructType, fieldName : String) : StructField = {
    val fields = fieldName.split("\\.")
    val field = typ(fields.head)
    fields.tail.foldLeft(field)((sF, fN)=> sF.dataType.asInstanceOf[StructType](fN))
  }

  def elemStructType(structField : StructField) = {
    assert(structField.dataType.isInstanceOf[ArrayType])

    val aT = structField.dataType.asInstanceOf[ArrayType]
    aT.elementType match {
      case sT : StructType => sT
      case _ => StructType(Array(structField))
    }
  }

  def expression(fieldName : String) : Expression = {
    val fields = fieldName.split("\\.")
    val expr : Expression = UnresolvedAttribute(fields.head)
    fields.tail.foldLeft(expr)((expr, fN) => UnresolvedExtractValue(expr, Literal(fN)))
  }

}
