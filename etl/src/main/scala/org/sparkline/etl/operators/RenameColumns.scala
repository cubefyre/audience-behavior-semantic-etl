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

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.logical.Project


/**
 * Created by Jitender on 6/26/15.
 */

object RenameColumns {

  def dataFrame(input : DataFrame,
                columns : Map[String, String], removeOldCols : Boolean = true) : DataFrame = {

    /* New method */
    //val newColNames = columns.map(col => new Column(UnresolvedAttribute(col._1)).alias(col._2)).toList
    //val goalEventsDF = input.select(newColNames: _*)


    //val (oldColNames, newColNames) = columns.toSeq.unzip
    val oldColNames = columns.keySet
    //val projections : Seq[NamedExpression] = input.schema.map(f => UnresolvedAttribute(f.name))
    // filter out the columns that are going to be aliased and leave
    val projections : Seq[NamedExpression] = input.schema.filter(f => !oldColNames.contains(f.name)).map(f => UnresolvedAttribute(f.name))

    // Additional two lines - to avoid creating aliases for cols that don't exist in the schema
    //val oldColsExist = input.schema.filter(f => oldColNames.contains(f.name)).toList
    //val filteredColumns = columns filterKeys oldColsExist.toSet

    val newProjections = columns.map(col => Alias(UnresolvedAttribute(col._1), col._2)())
    val plan = Project(projections ++ newProjections, input.queryExecution.analyzed)
    val df = new DataFrame(input.sqlContext, plan)
    /*
     Create another projection which keeps just the col names we want
     */
    if(removeOldCols) {
      val keepColNames = columns.values.toList
      val finalProjections: Seq[NamedExpression] =
        df.schema.filter(f => keepColNames.contains(f.name)).map(f => UnresolvedAttribute(f.name))
      val newPlan = Project(finalProjections, df.queryExecution.analyzed)
      new DataFrame(input.sqlContext, newPlan)
    }
    else
      df
  }

}
