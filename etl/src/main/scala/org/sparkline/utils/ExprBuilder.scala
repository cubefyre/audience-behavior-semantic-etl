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

import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions._


trait ExprBuilder {

  def allPartitionWdwFrame = SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing)

  def windowFnInvocation(fnName: String,
                         fnArgs: Seq[Expression],
                         partitionSpec: Seq[Expression],
                         orderSpec: Seq[SortOrder],
                         alias: String,
                         frameSpecification: WindowFrame = allPartitionWdwFrame
                          ): NamedExpression = {
    val wdwSpec = WindowSpecDefinition(partitionSpec, orderSpec, frameSpecification)
    val funcSpec = UnresolvedWindowFunction(fnName, fnArgs)
    Alias(WindowExpression(funcSpec, wdwSpec), alias)()
  }

  def rank(partitionSpec: Seq[Expression],
           orderSpec: Seq[SortOrder],
           alias: String): NamedExpression = {
    windowFnInvocation("rank", Seq(), partitionSpec, orderSpec, alias, UnspecifiedFrame)
  }

  def rowNumber(partitionSpec: Seq[Expression],
                 orderSpec: Seq[SortOrder],
                 alias: String): NamedExpression = {
    windowFnInvocation("row_number", Seq(), partitionSpec, orderSpec, alias, UnspecifiedFrame)
  }

  def firstValue(valueExpr: Expression,
                 partitionSpec: Seq[Expression],
                 orderSpec: Seq[SortOrder],
                 alias: String,
                 wdwFrame: WindowFrame = allPartitionWdwFrame): NamedExpression = {
    windowFnInvocation("first_value", Seq(valueExpr), partitionSpec, orderSpec, alias, wdwFrame)
  }

  def lastValue(valueExpr: Expression,
                 partitionSpec: Seq[Expression],
                 orderSpec: Seq[SortOrder],
                 alias: String,
                 wdwFrame: WindowFrame = allPartitionWdwFrame): NamedExpression = {
    windowFnInvocation("last_value", Seq(valueExpr), partitionSpec, orderSpec, alias, wdwFrame)
  }

  def countStar(valueExpr: Expression,
                partitionSpec: Seq[Expression],
                orderSpec: Seq[SortOrder],
                alias: String,
                wdwFrame: WindowFrame = allPartitionWdwFrame): NamedExpression = {
    //windowFnInvocation("count", Seq(UnresolvedStar(None)), partitionSpec, orderSpec, alias, wdwFrame)
    windowFnInvocation("count", Seq(valueExpr), partitionSpec, orderSpec, alias, wdwFrame)
  }

  def pagePath(valueExpr: Expression,
                partitionSpec: Seq[Expression],
                orderSpec: Seq[SortOrder],
                alias: String,
                wdwFrame: WindowFrame = allPartitionWdwFrame): NamedExpression = {
    windowFnInvocation("collect_list", Seq(valueExpr), partitionSpec, orderSpec, alias, wdwFrame)
  }

  def ntile(valueExpr: Expression,
           partitionSpec: Seq[Expression],
           orderSpec: Seq[SortOrder],
           alias: String,
           wdwFrame: WindowFrame = allPartitionWdwFrame): NamedExpression = {
    windowFnInvocation("ntile", Seq(valueExpr), partitionSpec, orderSpec, alias, wdwFrame)
  }

}
