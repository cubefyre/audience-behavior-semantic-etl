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
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedExtractValue, UnresolvedFunction}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{RepartitionByExpression, SortPartitions, Generate, LogicalPlan}
import org.apache.spark.sql.types.{StringType, Metadata, DataType, StructField}
import org.sparkline.etl.ETLExpressions

import scala.collection.AbstractTraversable

package session {

import org.apache.spark.sql.catalyst.CatalystTypeConverters

class T extends Traversable[Row] {
  @transient
  private[session] var row: Row = null

  override def foreach[U](f: (Row) => U): Unit = f(row)
}

case class eval() extends Function[Row, TraversableOnce[Row]] {

  val userIdCol = 0
  val sessIdCol = 1
  val sessMatchCol = 2

  @transient
  private var currUserId: Any = null

  @transient
  private var currSessionId: Any = null

  @transient
  private var sessRow = new GenericMutableRow(Array[Any](currSessionId))

  //@transient
  //private val t = new T

  def apply(iRow: Row): TraversableOnce[Row] = {

    val userId = iRow.get(userIdCol)

    if (currUserId == null || currUserId != userId) {
      currUserId = userId
      currSessionId = null
    }

    if (currSessionId == null || !(iRow.get(sessMatchCol).asInstanceOf[Boolean])) {
      currSessionId = iRow.get(sessIdCol)
    }

    if (sessRow == null) {
      sessRow = new GenericMutableRow(Array[Any](currSessionId))
    }

    val sessionIdTye = iRow.schema(sessIdCol).dataType

    sessRow.update(0, CatalystTypeConverters.convertToCatalyst(currSessionId, sessionIdTye))

    //t.row = sessRow

    Seq(sessRow)
  }

}

}

object Sessionize {

  def apply(input: LogicalPlan,
            partitionBy: Seq[Expression],
            orderBy: Seq[SortOrder],
            sessionMatchExpression: Predicate,
            userIdExpression: Expression,
            sessionIdExpression: Expression,
            sessionIdAttrName: String = "session_id"): LogicalPlan = {

    val plan = SortPartitions(orderBy, RepartitionByExpression(partitionBy, input))
    val e = new session.eval()
    val names = Seq(sessionIdAttrName)

    val elemTypes = Seq((StringType, false))
    val childExprs = Seq(userIdExpression, sessionIdExpression, sessionMatchExpression) ++
      input.schema.map(f => UnresolvedAttribute(f.name))

    val generator = UserDefinedGenerator(elemTypes, e, childExprs)
    Generate(generator, join = true, outer = false,
      qualifier = None, names.map(UnresolvedAttribute(_)), plan)

  }

  def dataFrame(input: DataFrame,
                partitionBy: Seq[Expression],
                orderBy: Seq[SortOrder],
                sessionMatchExpression: Predicate,
                userIdExpression: Expression,
                sessionIdExpression: Expression,
                sessionIdAttrName: String = "sd_session_id"): DataFrame = {
    new DataFrame(input.sqlContext,
      apply(input.queryExecution.analyzed,
        partitionBy, orderBy, sessionMatchExpression, userIdExpression, sessionIdExpression, sessionIdAttrName))
  }

  def dataFrame(input: DataFrame,
                eTLExpressions: ETLExpressions): DataFrame = {
    dataFrame(input,
      eTLExpressions.partitionByAnonymousCookie,
      eTLExpressions.sortByTimestamp,
      eTLExpressions.sessionMatchExpression,
      UnresolvedAttribute(eTLExpressions.anonymousCookieAttr),
      eTLExpressions.sessionIdExpression,
      eTLExpressions.sessionIdAttr)
  }
}
