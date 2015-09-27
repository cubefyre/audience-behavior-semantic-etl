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

 package org.sparkline.etl

import org.apache.spark.sql.catalyst.analysis.UnresolvedExtractValue
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}

object SessionMetricsTest {

  import org.apache.spark.sql.catalyst.dsl.expressions._


  val session_id = UnresolvedExtractValue('session, "id").as("session_id")

  val metricsDefinitions : Map[String, Expression] = Map(
    "startTime" -> min(UnresolvedExtractValue('event, "timestamp")),
    "endTime" -> max(UnresolvedExtractValue('event, "timestamp")),
    "sessionDuration" -> ('endTime - 'startTime),
    "numEvents" -> count('event),
    "numScreens" -> countDistinct(UnresolvedExtractValue('screen, "name"))
  )

  val metrics : Seq[NamedExpression] = metricsDefinitions.map {
    case (nm, e) => e.as(nm)
  }.toSeq
}
