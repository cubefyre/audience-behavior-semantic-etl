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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.sparkline.etl.ETLExpressions


object Lag {

  def dataFrame(input : DataFrame,
                partitionBy : Seq[Expression],
                orderBy : Seq[SortOrder],
                lagExpression : Expression,
                 lagAmt : Int,
                 lagAttrName : String) : DataFrame = {
    import org.apache.spark.sql.catalyst.dsl.plans._

    val inputPlan = input.queryExecution.analyzed

    val attrs = inputPlan.schema.map(f => UnresolvedAttribute(f.name))

    val wdwSpec = WindowSpecDefinition(partitionBy, orderBy, UnspecifiedFrame)

    val lagFunction = UnresolvedWindowFunction("lag", Seq(lagExpression, Literal(lagAmt)))

    val wdwExpr : NamedExpression = Alias(WindowExpression(lagFunction, wdwSpec), lagAttrName)()

    val lagPlan = inputPlan.select((attrs :+ wdwExpr):_*)

    new DataFrame(input.sqlContext, lagPlan)
  }

  def lagOnTimeStamp(input: DataFrame,
                     eTLExpressions: ETLExpressions,
                     lagAmt: Int): DataFrame = {
    val partitionBy = eTLExpressions.partitionByAnonymousCookie
    val orderBy = eTLExpressions.sortByTimestamp
    val timestampAttr = eTLExpressions.timestampAttr
    val lagAttrName = eTLExpressions.prevEventTimeAttr

    dataFrame(input,
      partitionBy,
      orderBy,
      UnresolvedAttribute(timestampAttr),
      lagAmt,
      lagAttrName)
  }
}
