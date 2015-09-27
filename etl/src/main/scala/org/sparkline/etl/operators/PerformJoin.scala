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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.{JoinType, Inner}
import org.apache.spark.sql.catalyst.plans.logical.{Subquery, Join}

/**
 * Created by Jitender on 7/29/15.
 */
object PerformJoin {

    def dataFrame(input1 : DataFrame, input2: DataFrame,
                  joinOnColumns : Tuple2[String, String],
                  operator : String = "eq",
                  joinType : JoinType = Inner,
                  removeMappedCols : Boolean = false) : DataFrame = {

      val leftColExpr = UnresolvedAttribute("i1." + joinOnColumns._1)
      val rightColExpr = UnresolvedAttribute("i2." + joinOnColumns._2)

      val joinExpr : Expression = operator match {
        case "eq" => EqualTo(leftColExpr, rightColExpr)
        case "lt" => LessThan(leftColExpr, rightColExpr)
        case "lte" => LessThanOrEqual(leftColExpr, rightColExpr)
        case "gt" => GreaterThan(leftColExpr, rightColExpr)
        case "gte" => GreaterThanOrEqual(leftColExpr, rightColExpr)
      }

      val plan = Join(
        Subquery("i1", input1.queryExecution.logical),
        Subquery("i2", input2.queryExecution.logical),
        joinType,
        Some(joinExpr)
      )
      new DataFrame(input2.sqlContext, plan)
    }

    def dataFrameEx(input1 : DataFrame, alias1 : String = "i1",
                    input2: DataFrame, alias2 : String = "i2",
                    joinExpr : Expression,
                    joinType : JoinType = Inner
                    ) : DataFrame = {

      val plan = Join(
        Subquery(alias1, input1.queryExecution.logical),
        Subquery(alias2, input2.queryExecution.logical),
        joinType,
        Some(joinExpr)
      )
      new DataFrame(input2.sqlContext, plan)
    }
  }
