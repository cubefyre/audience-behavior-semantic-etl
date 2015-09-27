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

 package org.sparkline.etl.operators
 /**
 * Created by Harish.
 */
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.plans.logical.Generate
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.StructType
import org.sparkline.utils.SchemaUtils

object FlattenArrayField {

  def dataFrame(input : DataFrame,
                fieldName : String,
                fieldPrefix : String
                ) : DataFrame = {
    val prd = SchemaUtils.expression(fieldName)
    val inputSchema = input.schema
    val outColType : StructType = SchemaUtils.elemStructType(SchemaUtils.field(inputSchema, fieldName))

    val elementTypes = outColType.map { attr => (attr.dataType, attr.nullable) }
    val names = outColType.map(n => s"${fieldPrefix}_${n.name}")

    val rowFunction : Row => TraversableOnce[Row] = { r : Row =>
      val v = r.get(0)
      v match {
        case l : List[_] => {
          l.asInstanceOf[List[Row]].filter(_ != null).map(e => CatalystTypeConverters.convertToCatalyst(e).asInstanceOf[Row])
        }
        case _ => Seq()
      }
    }

    val generator = UserDefinedGenerator(elementTypes, rowFunction, Seq(prd))

    val flattenPlan = Generate(generator, join = true, outer = true,
      qualifier = None, names.map(UnresolvedAttribute(_)), input.queryExecution.analyzed)

    new DataFrame(input.sqlContext, flattenPlan)
  }

}