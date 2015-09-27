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

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.hadoop.hive.ql.exec.FunctionRegistry
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver
import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, RepartitionByExpression, SortPartitions, Project}
import org.joda.time.DateTime
import org.scalatest.{GivenWhenThen, BeforeAndAfterAll}
import org.sparkline.base.{TestHive, QueryTest}
import org.sparkline.utils.ResourceUtils

class ManualPlanTests extends QueryTest with BeforeAndAfterAll with GivenWhenThen with Logging {

  import TestHive.{read, sql, udf}

  var eventsDir : File = _

  override def beforeAll() = {

    udf.register("spklDateTme", (s : String) => new DateTime(s).getMillis)

    val cls = Class.forName("org.sparkline.hive.udafs.GenericUDAFSumMaps").asInstanceOf[Class[GenericUDAFResolver]]

    FunctionRegistry.registerGenericUDAF(false,
      "sum_maps",
      cls.newInstance())

    eventsDir = ResourceUtils.createTempDir()
    FileUtils.copyInputStreamToFile(getClass.getResource("/events.json.gz").openStream(),
      new File(eventsDir, "events.json.gz"))
  }

  ignore("testRepartition") {

    val jsonDF = read.json(eventsDir.getCanonicalPath)

    import org.apache.spark.sql.catalyst.dsl.expressions._

    val sessionPartitioning = Seq(UnresolvedExtractValue('session, "id"))
    val sessionOrdering = Seq(
      SortOrder(UnresolvedExtractValue('session, "id"), Ascending),
      SortOrder(UnresolvedExtractValue('event, "timestamp"), Ascending)
    )

    val plan = SortPartitions(sessionOrdering, RepartitionByExpression(sessionPartitioning, jsonDF.queryExecution.logical))
    var df = new DataFrame(jsonDF.sqlContext, plan)

    df.take(100).foreach { r =>
      val sess = r.getAs[Row]("session")
      val evnt = r.getAs[Row]("event")
      println(sess.getAs("id").toString + ", " + evnt.getAs("timestamp").toString)
    }
  }

  test("testMetricAnalyzer") {

    val jsonDF = read.json(eventsDir.getCanonicalPath)

    import org.apache.spark.sql.catalyst.dsl.expressions._

    val sessionPartitioning = Seq(UnresolvedExtractValue('session, "id"))
    val sessionOrdering = Seq(
      SortOrder(UnresolvedExtractValue('session, "id"), Ascending),
      SortOrder(UnresolvedExtractValue('event, "timestamp"), Ascending)
    )

    val plan = SortPartitions(sessionOrdering, RepartitionByExpression(sessionPartitioning, jsonDF.queryExecution.logical))
    var df = new DataFrame(jsonDF.sqlContext, plan)

    val analysisLayer : AnalysisLayer =
      MetricResolver.resolve(SessionMetricsTest.metrics, df.queryExecution.logical, caseInsensitiveResolution)

    println(analysisLayer.treeString())

  }

  ignore("testSessionETL") {

    val jsonDF = read.json(eventsDir.getCanonicalPath)

    import org.apache.spark.sql.catalyst.dsl.expressions._

    val sessionPartitioning = Seq(UnresolvedExtractValue('session, "id"))
    val sessionOrdering = Seq(
      SortOrder(UnresolvedExtractValue('session, "id"), Ascending),
      SortOrder(UnresolvedExtractValue('event, "timestamp"), Ascending)
    )

    val plan = SortPartitions(sessionOrdering, RepartitionByExpression(sessionPartitioning, jsonDF.queryExecution.logical))
    var df = new DataFrame(jsonDF.sqlContext, plan)

    val analysisLayer : AnalysisLayer =
      MetricResolver.resolve(SessionMetricsTest.metrics, df.queryExecution.logical, caseInsensitiveResolution)

    val firstAnalysisLayer = analysisLayer.prev.left.get

    val sessionETLPlan = new Aggregate(Seq(SessionMetricsTest.session_id),
      Seq(SessionMetricsTest.session_id) ++ firstAnalysisLayer.namedMetrics,
      plan)

    df = new DataFrame(jsonDF.sqlContext, sessionETLPlan)

    println(df.queryExecution.sparkPlan.treeString)

    df.take(50).foreach { r =>
      println(r)
    }

  }
  ignore("test1.0") {

    val jsonDF = read.json(eventsDir.getCanonicalPath)

    jsonDF.take(1).foreach { r =>
      r.schema.printTreeString()
      println(r)
      r
    }
  }

  ignore("testProjection") {

    val jsonDF = read.json(eventsDir.getCanonicalPath)

    import org.apache.spark.sql.catalyst.dsl.expressions._

    val projections = Seq(
      UnresolvedExtractValue('session, "id").as("session_id"),
      UnresolvedFunction("spklDateTme", Seq(UnresolvedExtractValue('event, "timestamp"))).as("dayOfYear")
    )

    val plan = Project(projections, jsonDF.queryExecution.logical)
    val pdf = new DataFrame(jsonDF.sqlContext, plan)
    pdf.take(100).foreach { r =>
      println(r)
    }
  }

  ignore("test1") {

    val jsonDF = read.json(eventsDir.getCanonicalPath)

    jsonDF.take(1).foreach { r =>
      r.schema.printTreeString()
      println(r)
      r
    }

    import org.apache.spark.sql.catalyst.dsl.expressions._

    udf.register("getMillis", {(s : String) => new DateTime(s).getMillis()})
    //val getDOY = udf {(s : String) => new DateTime(s).dayOfYear().get}

    min(UnresolvedFunction("getMillis", Seq(UnresolvedExtractValue('event, "timestamp")) )) as "StartTime"

    // project session.id, dayOfYear
    if ( 1 == 2 )
    {
      val projections = Seq(
        UnresolvedExtractValue('session, "id").as("session_id"),
        UnresolvedFunction("spklDateTme", Seq(UnresolvedExtractValue('event, "timestamp"))).as("dayOfYear")
      )

      val plan = Project(projections, jsonDF.queryExecution.logical)
      val pdf = new DataFrame(jsonDF.sqlContext, plan)
      pdf.take(100).foreach { r =>
        println(r)
      }

    }

    // session level repartition

    val sessionPartitioning = Seq(UnresolvedExtractValue('session, "id"))
    val sessionOrdering = Seq(
      SortOrder(UnresolvedExtractValue('session, "id"), Ascending),
      SortOrder(UnresolvedExtractValue('event, "timestamp"), Ascending)
    )

    val plan = SortPartitions(sessionOrdering, RepartitionByExpression(sessionPartitioning, jsonDF.queryExecution.logical))
    var df = new DataFrame(jsonDF.sqlContext, plan)


    val analysisLayer : AnalysisLayer =
      MetricResolver.resolve(SessionMetricsTest.metrics, df.queryExecution.logical, caseInsensitiveResolution)

    println(analysisLayer.treeString())

    val firstAnalysisLayer = analysisLayer.prev.left.get

    val sessionETLPlan = new Aggregate(Seq(SessionMetricsTest.session_id),
      Seq(SessionMetricsTest.session_id) ++ firstAnalysisLayer.namedMetrics,
      plan)

    df = new DataFrame(jsonDF.sqlContext, sessionETLPlan)

    println(df.queryExecution.sparkPlan.treeString)

    df.take(50).foreach { r =>
      println(r)
    }



    // Attempt 1
    // SessionETL injected as a MapPartitions step
    // failed attempt
    // fails in Serialization issue + brittle + when running Aggregate Plan inside SessionETL there is no TaskContext
    if ( 1 == 2 ) {
      val sETL = new SessionETL(analysisLayer.prev.left.get, df.queryExecution.logical.schema)

      val sessionETLDF = df.mapPartitions(i => {
        sETL(i.asInstanceOf[Iterator[GenericRowWithSchema]])
      })

      sessionETLDF.take(5).foreach { r =>
        println(r)
      }
    }


    // SessionETL is a fn from Iterator[GenericRowWithSchema] => Iterator[GenericRowWithSchema]
    // Original structure
    /*
    root
 |-- event: struct (nullable = true)
 |    |-- id: string (nullable = true)
 |    |-- name: string (nullable = true)
 |    |-- timestamp: string (nullable = true)
 |    |-- value: string (nullable = true)
 |-- referral: struct (nullable = true)
 |    |-- ad: string (nullable = true)
 |    |-- adGroup: string (nullable = true)
 |    |-- campaign: string (nullable = true)
 |    |-- url: string (nullable = true)
 |-- screen: struct (nullable = true)
 |    |-- name: string (nullable = true)
 |-- session: struct (nullable = true)
 |    |-- id: long (nullable = true)
 |-- user: struct (nullable = true)
 |    |-- ageBand: string (nullable = true)
 |    |-- firstName: string (nullable = true)
 |    |-- gender: string (nullable = true)
 |    |-- homeAddress: struct (nullable = true)
 |    |    |-- address: string (nullable = true)
 |    |    |-- latitude: double (nullable = true)
 |    |    |-- longitude: double (nullable = true)
 |    |-- id: string (nullable = true)
 |    |-- lastName: string (nullable = true)
 |-- user-agent: struct (nullable = true)
 |    |-- device: struct (nullable = true)
 |    |    |-- model: string (nullable = true)
 |    |    |-- osVersion: string (nullable = true)
 |    |    |-- type: string (nullable = true)
 |    |    |-- version: string (nullable = true)

     */

    // Output Structure
    /*
    session :
      id
      metrics :
        metricName : { value : ???, rabnk  ntile
      events : []
     */

  }


  //  def rowToJson(row : GenericRowWithSchema, hasNext : Boolean): String = {
  //    //
  //    val writer = new CharArrayWriter()
  //    val gen = new JsonFactory().createGenerator(writer).setRootValueSeparator(null)
  //
  //    JsonRDD.rowToJSON(row.schema, gen)(row)
  //
  //    gen.flush()
  //
  //    val json = writer.toString
  //    if (hasNext) {
  //      writer.reset()
  //    } else {
  //      gen.close()
  //    }
  //
  //    json
  //  }


}
