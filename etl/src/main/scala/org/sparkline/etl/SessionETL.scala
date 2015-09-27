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
 package org.sparkline.etl

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{Aggregate, PhysicalRDD}
import org.apache.spark.sql.types._


class SessionETL(val analysisLayer: AnalysisLayer, inputSchema : StructType)
  extends (Iterator[GenericRowWithSchema] => Iterator[GenericRowWithSchema]) with Serializable {

  //val sessionId = GetField AttributeReference("id", StringType)

  val sessionField : StructField = inputSchema("session")
  val sessionId = new Alias(ExtractValue(
    AttributeReference("session", sessionField.dataType, false)(),
    Literal("id"), caseInsensitiveResolution), "session_id")()

  val groupingExprs = Seq(sessionId)


  override def apply(v1: Iterator[GenericRowWithSchema]): Iterator[GenericRowWithSchema] = {

    ???

//    val attrs : Seq[Attribute] =
//      inputSchema.fields.map{f => new AttributeReference(f.name, f.dataType)()}.toSeq
//
//    val baseNode = PhysicalRDD(attrs, new IteratorBasedRDD(SparkContext.getOrCreate(), v1))
//    val aggNode = Aggregate(
//      true,
//      groupingExprs,
//      groupingExprs ++ analysisLayer.metricsSchema,
//        baseNode)
//
//
//    val outRDD = aggNode.execute()
//
//    outRDD.compute(new MetricsPartition(0), null).asInstanceOf[Iterator[GenericRowWithSchema]]
  }
}
