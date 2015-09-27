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
import org.joda.time.format.DateTimeFormatter
import org.joda.time.{Seconds, DateTime}
import com.github.nscala_time.time.Imports._

import org.apache.commons.io.FileUtils
import org.apache.hadoop.hive.ql.exec.FunctionRegistry
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver
import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedExtractValue}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.And
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.hive.{HiveGenericUdtf, HiveContext, HiveShim}
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.test.TestSQLContext._
import org.apache.spark.sql.types.{StringType, IntegerType, StructType}
import org.apache.spark.sql.{Row, SaveMode, DataFrame}

import org.scalatest.{BeforeAndAfterAll, GivenWhenThen}
import org.sparkline.base.{TestHive, QueryTest}
import org.sparkline.customers.acme.{DailyCubesWithLookbackETL, DailyCubesETL, AcmeETLExpressions}
import org.sparkline.etl.operators._
import org.sparkline.utils.{ResourceUtils,SchemaUtils}

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst._

import org.sparklinedata.spark.dateTime.dsl.expressions._
import org.sparklinedata.spark.dateTime.Functions
import org.sparkline.utils.ExprBuilder


import org.apache.spark.sql.DataFrame

class AcmeETLTest extends QueryTest with BeforeAndAfterAll with GivenWhenThen with Logging {

  import TestHive.{read, sql, udf}

  var eventsDir : File = _

  override def beforeAll() = {

    functions.register(TestHive)
    Functions.register(TestHive)

    eventsDir = ResourceUtils.createTempDir()
    FileUtils.copyInputStreamToFile(getClass.getResource("/acme.json.gz").openStream(),
      new File(eventsDir, "events.json.gz"))
  }

  test("testS3") {
    /*
     * to run this test, set aws credentials:
     * export AWS_ACCESS_KEY_ID=...
     * export AWS_SECRET_ACCESS_KEY=...
     */

    val jsonDF = read.json("s3n://acme.logs.segment.production/segment-logs/VhBYQcAjH5/1441065600000/*.gz")
    //val jsonDF = read.json(eventsDir.getCanonicalPath)
    jsonDF.printSchema()
    jsonDF.take(10).foreach { r =>
      r.schema.printTreeString()
      println(r)
      r
    }

    println(jsonDF.count())
  }

  ignore("acmeCsv") {

    val jsonDF = read.json(eventsDir.getCanonicalPath)

    jsonDF.registerTempTable("eventLog")
    val eL = sql( """select
        anonymousId,
         channel,
         context.campaign.content as campaign_content, context.campaign.medium as campaign_medium,
         context.campaign.name as campaign_name, context.campaign.source as campaign_source,
         context.ip as ip,
         context.library.name as library_name,
         context.library.version as library_version,
         context.page.path as page_path,
         context.page.referrer as page_referrer,
         context.page.search as page_search,
         context.page.title as page_tile,
         context.page.url as page_url,
         context.userAgent as userAgent,
         event,
         integrations,
         messageId,
         originalTimestamp,
         projectId,
         properties.couponcode as couponcode,
         properties.currency as currency,
         properties.discount as discount,
         properties.orderId as orderId,
         properties.path as path,
         properties.products[0].price as product_price,
         properties.products[0].productname as product_productname,
         properties.products[0].quantity as product_quantity,
         properties.referrer as referrer,
         properties.revenue as revenue,
         properties.search as search,
         properties.shipping as shipping,
         properties.tax as tax,
         properties.title as title,
         properties.url as url,
         receivedAt,
         sentAt,
         timestamp,
         type,
         version
        from eventLog""")
    val rdd = eL.repartition(1).map(_.mkString(":"))
    //rdd.saveAsTextFile("/tmp/el")
  }

  ignore("acmeSave") {

    val jsonDF = read.json(eventsDir.getCanonicalPath)
    val eL = jsonDF.repartition(1).cache
    eL.write.mode(SaveMode.Overwrite).json("/tmp/acme.json")

  }

  ignore("acmeLoad") {

    val jsonDF = read.json(eventsDir.getCanonicalPath)
    println(jsonDF.count)

  }

  ignore("acmePrintSchema") {

    val jsonDF = read.json(eventsDir.getCanonicalPath)

    jsonDF.queryExecution.analyzed.printSchema()
  }

  ignore("acmeProject") {

    val jsonDF = read.json(eventsDir.getCanonicalPath)

    val eL = jsonDF.repartition(1).cache
    eL.registerTempTable("eventLog")

    val pDF = eL.select(AcmeETLExpressions.anonymousCookieAttr, AcmeETLExpressions.timestampAttr)

    pDF.take(5).foreach { r =>
      //r.schema.printTreeString()
      println(r)
      r
    }

  }

  ignore("acmeLagPlan") {

    val jsonDF = read.json(eventsDir.getCanonicalPath)

    val eL = jsonDF.repartition(1).cache
    eL.registerTempTable("eventLog")

    val lagDF = Lag.dataFrame(eL,
      AcmeETLExpressions.partitionByAnonymousCookie,
      AcmeETLExpressions.sortByTimestamp,
      AcmeETLExpressions.timestampAttr,
      1,
    AcmeETLExpressions.prevEventTimeAttr)

    lagDF.take(1).foreach { r =>
      r.schema.printTreeString()
      println(r)
      r
    }

  }

  ignore("acmeFlatten") {
    val jsonDF = read.json(eventsDir.getCanonicalPath)
    val eL = DropColumns.dataFrame(jsonDF.repartition(1).cache,
      Seq("integrations"))

    val fieldName = "properties.products"
    val fieldPrefix = "product"

    val flattenDF = FlattenArrayField.dataFrame(eL, fieldName, fieldPrefix)
    flattenDF.printSchema()
    println(eL.count)
    println(flattenDF.count)
    flattenDF.write.mode(SaveMode.Overwrite).parquet("/tmp/flattened.parquet")

  }

  test("acmeSessionizePlan") {

    val jsonDF = read.json(eventsDir.getCanonicalPath)
    //val jsonDF = read.json("s3n://acme.logs.segment.production/segment-logs/*/*/*.gz")
    val eL = jsonDF.repartition(1).cache
    //eL.registerTempTable("eventLog")

    val lagDF = Lag.lagOnTimeStamp(eL,
      AcmeETLExpressions,
    1)

    val sessionizeDF = Sessionize.dataFrame(lagDF,
      AcmeETLExpressions)

    sessionizeDF.take(100).foreach { r =>
      //r.schema.printTreeString()
      println(r)
      r
    }

    //val c = new Column(UnresolvedAttribute(AcmeETLExpressions.sessionIdAttrName)
//
    val sessCnt = sessionizeDF.groupBy(AcmeETLExpressions.sessionIdAttr).count()
    sessCnt.foreach { r =>
      //r.schema.printTreeString()
      println(r)
    }

    println(sessCnt.queryExecution)

  }

  test("acmeSessionDailyMetrics") {
    def etlExpressions: ETLExpressions = AcmeETLExpressions

    // Step 1 - read raw events
    val rawEvents: ETLStep = ETLStep("rawEvent", "read raw events",
      read.json("/tmp/1436745600000"), Seq())
    /*
    rawEvents.dataFrame.registerTempTable("tom")
    val rDF : DataFrame = sql(
      "select originalTimestamp, timestamp from tom")
      */

    // Step 2 - Multiple enrichment steps
    // Flatten product array, rename columns, remove bad fields,
    val enrichedRawEvents: ETLStep = ETLStep("enrichRawEvents", "embelish raw events",
      DropColumns.dataFrame(
          AddColumns.dataFrame(
            RenameColumns.dataFrame(
              FlattenArrayField.dataFrame(rawEvents.output, "properties.products", "sd_product"),
              Map(
                "anonymousId" -> "user_id",
                "category" -> "category",
                "channel" -> "channel",
                "context.campaign.name" -> "campaign_name",
                "context.campaign.content" -> "campaign_content",
                "context.campaign.medium" -> "campaign_medium",
                "context.campaign.source" -> "campaign_source",
                "context.campaign.term" -> "campaign_term",
                "originalTimestamp" -> "original_timestamp",
                "context.ip" -> "ip_address",
                "context.page.title" -> "page_name",
                "context.page.path" -> "page_path",
                "context.page.referrer" -> "page_referrer",
                "context.page.search" -> "page_params",
                "context.page.url" -> "page_url",
                "context.userAgent" -> "user_agent",
                "messageId" -> "event_id",
                "event" -> "event_name",
                "name" -> "name",
                "projectid" -> "project_id",
                "properties.search" -> "event_param",
                "properties.path" -> "event_path",
                "properties.referrer" -> "event_referrer",
                "properties.url" -> "event_url",
                "properties.title" -> "event_title",
                "properties.product" -> "product",
                "properties.currency" -> "currency",
                "properties.orderId" -> "order_id",
                "properties.couponcode" -> "order_coupon_code",
                "properties.revenue" -> "order_revenue",
                "properties.discount" -> "order_discount",
                "properties.tax" -> "ordertax",
                "properties.shipping" -> "order_shipping",
                "properties.emailId" -> "order_email",
                "properties.name" -> "property_name",
                "properties.sessionid" -> "segment_session_id",
                "receivedAt" -> "received_at",
                "sentAt" -> "segment_sent_at",
                "timestamp" -> "utc_time",
                "type" -> "type",
                "userId" -> "segment_user_id",
                "sd_product_price" -> "sd_product_price",
                "sd_product_productname" -> "sd_product_name",
                "sd_product_quantity" -> "sd_product_quantity"
              ),true),
            Seq(etlExpressions.parseYearExpr,
              etlExpressions.parseMonthExpr, etlExpressions.parseDayExpr, etlExpressions.parseDayNameExpr,
              etlExpressions.parseHourOfDayExpr)
          ), Seq("integrations","sd_ip_address", "sd_user_agent")), Seq(rawEvents))

    // Adding a bunch of case statements
    val productCategoryCaseBranches = List( ("sd_product_name","%AcmeAntiAging%", "AntiAging"),
      ("sd_product_name","%AcmeLaserHair%", "LHR"),
      ("sd_product_name","%AcmeAcne%", "Acne"),
      ("sd_product_name","%AcmeHRL%", "HRL Precision")
    )
    val productCategoryCSElseValue = "Other"
    val productCategoryCSAttrName = "sd_product_category"

    val ce1 = CaseStatement.caseUsingLike(productCategoryCaseBranches, productCategoryCSElseValue, productCategoryCSAttrName)
    val ce2 = CaseStatement.caseUsingEquals(List(("event_name", "Completed Order", 1)), 0, IntegerType, "sd_is_revenue_event")
    val ce3 = CaseStatement.caseUsingEquals(List(("event_name", "AddToBag", 1)), 0, IntegerType, "sd_is_cart_event")
    val ce4 = CaseStatement.caseUsingEquals(List(("event_name", "Click Video", 1)), 0, IntegerType, "sd_is_video_event")

    val enrichedRawEventsWithRevenue = ETLStep("addCaseStatement", "Generate New Columns From Case Classes",
      AddColumns.dataFrame(enrichedRawEvents.output, Seq(ce1,ce2,ce3,ce4, etlExpressions.revenueExpr)),
      Seq(enrichedRawEvents)
    )

    val enrichedRawEventsWithPreviousTimestamp = ETLStep("rawEventsWithPreviousTimestamp",
      "add in previous event timestamp",
      Lag.lagOnTimeStamp(enrichedRawEventsWithRevenue.output,
        AcmeETLExpressions,
        1),
      Seq(enrichedRawEventsWithRevenue))

    //enrichedRawEventsWithPreviousTimestamp.dataFrame.printSchema()


    val sessionizedRawEvents = ETLStep("sessionizedRawEvents",
      "Sessionize Event Stream",
      Sessionize.dataFrame(enrichedRawEventsWithPreviousTimestamp.output,
        AcmeETLExpressions),
      Seq(enrichedRawEventsWithPreviousTimestamp),
      true,
      Seq("sd_year", "sd_month", "sd_day"))

    sessionizedRawEvents.dataFrame.registerTempTable("tem")

    val sessionMetrics = ETLStep("sessionMetrics",
        "Create Session Metrics From Sessionize Event Stream",
          FilterRows.dataFrame(
            AddColumns.dataFrame(
              sessionizedRawEvents.output,
              Seq(AcmeETLExpressions.sessionRowNumberByTimeExpr,
                AcmeETLExpressions.sessionStartTimeExpr,
                AcmeETLExpressions.sessionEndTimeExpr,
                AcmeETLExpressions.sessionEventCountExpr,
                AcmeETLExpressions.sessionLandingScreenExpr,
                AcmeETLExpressions.sessionExitScreenExpr,
                AcmeETLExpressions.sessionPagePathExpr,
                AcmeETLExpressions.sessionPagesExpr,
                AcmeETLExpressions.sessionEventPathExpr,
                AcmeETLExpressions.sessionPageReferrerExpr,
                AcmeETLExpressions.sessionProductsExpr,
                AcmeETLExpressions.sessionProductQuantityExpr,
                AcmeETLExpressions.sessionRevenueExpr,
                AcmeETLExpressions.isRevSessionExpr,
                AcmeETLExpressions.isCartSessionExpr,
                AcmeETLExpressions.isVideoSessionExpr,
                AcmeETLExpressions.sessionDurationExpr,
                AcmeETLExpressions.isBounceSessionExpr,
                AcmeETLExpressions.avgTimePerEventExpr
              )
              ),
            AcmeETLExpressions.filterSessionRows
          ),
        Seq(sessionizedRawEvents),
      true,
      Seq("sd_year", "sd_month", "sd_day"))

    //sessionMetrics.dataFrame.printSchema()
    //println(sessionMetrics.queryExecution.analyzed)
    sessionMetrics.dataFrame.write.mode(SaveMode.Overwrite).parquet("/tmp/acme/sd_sessions")
    sessionMetrics.dataFrame.registerTempTable("tem")
    val gDF : DataFrame = sql(
      "select sd_session_id, sd_session_rank, sd_session_start_time, sd_session_end_time, sd_session_num_events, sd_session_page_path from tem")
    gDF.take(10).foreach(println)

  }

  test("acmeFull") {

    val daysToRun : Seq[Long] = Seq()

    var startTime = org.joda.time.DateTime.now()

    //val rawDataBasePath = "/tmp/acme/raw/{1437955200000,1438041600000,1438128000000}"
    val rawDataBasePath = "/tmp/acme/raw/1439856000000"
    //val dayInMills = 1436745600000L

    val etlBasePath = "/tmp/acme"

    val acme = new DailyCubesETL(rawDataBasePath, etlBasePath)(TestHive)

    val etl = new ETLProcess(acme)
    etl.run
  }

  test("acmePrintPlans") {

    val rawEvents = read.json(eventsDir.getCanonicalPath)

    val plan: LogicalPlan = rawEvents.queryExecution.analyzed
    println(plan)

    val optimizedPlan = rawEvents.queryExecution.optimizedPlan
    println(optimizedPlan)

    val physicalPlan = rawEvents.queryExecution.sparkPlan
    println(physicalPlan)
  }

  test("acmeDateTime") {

    /*
    day, week, month, year,
    val sessionPartitioning = Seq(UnresolvedExtractValue('session, "id"))
    val plan = SortPartitions(sessionOrdering, RepartitionByExpression(sessionPartitioning, jsonDF.queryExecution.logical))
    var df = new DataFrame(jsonDF.sqlContext, plan)
    */

    //val jsonDF = read.json(eventsDir.getCanonicalPath)
    //jsonDF.registerTempTable("tem")

    val rawEvents: ETLStep = ETLStep("rawEvent", "read raw events",  read.json("/tmp/acme/raw/1436745600000"), Seq())
    rawEvents.dataFrame.registerTempTable("tem")

    val dT = dateTime('timestamp)
    //val dOW = dateTime('segment_utc_time) dayOfWeek
    val dOWN = dateTime('timestamp) dayOfWeekName
    val dOM = dateTime('timestamp) dayOfMonth
    val dOY = dateTime('timestamp) dayOfYear
    val mOY = dateTime('timestamp) monthOfYear
    val yr = dateTime('timestamp) year
    val hOD = dateTime('timestamp) hourOfDay
    val millis = dateTime('timestamp) millis
    val tz = dateTime('timestamp) timeZoneId
    //val pst = dateTime('timestamp) withZone("US/Pacific")
    val dPST = (dateTime('timestamp) - 8.hour) withZone("US/Pacific")

    //val d = dateTime('dt) localDate
    //val timeBucket = dateTime('dt) bucket(start, 3.days)

    val t = sql(date"select originalTimestamp, timestamp, $dT, $tz, $dPST, $yr, $millis from tem limit 500")
    t.take(500).foreach(println)
  }

  ignore("acmeLag") {
    val jsonDF = read.json(eventsDir.getCanonicalPath)
    val eL = jsonDF.repartition(1).cache
    eL.registerTempTable("eventLog")
    val lagDF : DataFrame = sql(
      "select timestamp, lag(timestamp,1) over(partition by anonymousId order by timestamp) from eventLog")

    lagDF.take(10).foreach { r =>
      println(r)
      r
    }
  }

  ignore("acmeProductionOneDay") {

    val jsonDF = read.json("s3n://acme.logs.segment.production/segment-logs/*/1432684800000/*.gz")

    println(jsonDF.count())
  }

  ignore("testAcmeSessionize") {
    /*
     * to run this test, set aws credentials:
     * export AWS_ACCESS_KEY_ID=...
     * export AWS_SECRET_ACCESS_KEY=...
     */

    val jsonDF = read.json(eventsDir.getCanonicalPath)

    jsonDF.take(1).foreach { r =>
      r.schema.printTreeString()
      println(r)
      r
    }
  }

  ignore("testUserAgentInvocation") {

    val jsonDF = read.json(eventsDir.getCanonicalPath)

    import org.apache.spark.sql.catalyst.dsl.expressions._

    val projections = Seq(
      UnresolvedFunction("parseUserAgent", Seq(UnresolvedExtractValue('context, "userAgent"))).as("userAgent")
    )

    val pdf = AddColumns.dataFrame(jsonDF, projections)
    pdf.printSchema()
    pdf.take(5).foreach { r =>
      println(r)
    }
  }

  test("testIPInvocation") {

    val jsonDF = read.json(eventsDir.getCanonicalPath)

    import org.apache.spark.sql.catalyst.dsl.expressions._

    val projections = Seq(
      UnresolvedFunction("parseIp", Seq(UnresolvedExtractValue('context, "ip"))).as("ip")
    )

    val pdf = AddColumns.dataFrame(jsonDF, projections)
    pdf.printSchema()
    pdf.take(5).foreach { r =>
      println(r)
    }
  }

  test("testUrlTuple"){
    val jsonDF = read.json(eventsDir.getCanonicalPath)
    jsonDF.registerTempTable("tem")

    /*
      //parse URL and Generate host and other columns - DO NOT DELETE THIS CODE -

      val childrenExpr: Seq[Expression] = Seq(UnresolvedAttribute("event_referrer"), Literal("HOST"),
      Literal("PATH"), Literal("QUERY"), Literal("QUERY:utm_campaign"), Literal("QUERY:utm_source"),
        Literal("QUERY:utm_medium"), Literal("QUERY:utm_content"), Literal("QUERY:utm_term"))

      val outNames = Seq("sd_host","sd_path","sd_query", "sd_campaign", "sd_source", "sd_medium", "sd_content", "sd_term")


      val parseUrlGenerator = new HiveGenericUdtf(new HiveFunctionWrapper("org.apache.hadoop.hive.ql.udf.generic.GenericUDTFParseUrlTuple"),
        childrenExpr)

      val parseUrlPlan = Generate(parseUrlGenerator, join = true, outer = true,
        qualifier = None, outNames.map(UnresolvedAttribute(_)), rawEventsWithRevenue.dataFrame.queryExecution.analyzed)

      val af = new DataFrame(rawEventsWithRevenue.dataFrame.sqlContext, parseUrlPlan)
      af.printSchema()
     */
    val parseURLTuple = sql("""
      SELECT a.*, b.*
      from tem a LATERAL VIEW
      parse_url_tuple(a.properties.referrer, 'HOST', 'PATH', 'QUERY', 'QUERY:utm_campaign',
      'QUERY:utm_source', 'QUERY:utm_medium', 'QUERY:utm_content', 'QUERY:utm_term' ) b
      as sd_host, sd_path, sd_query, sd_campaign, sd_source_o, sd_medium_o, sd_content, sd_term
    """)
    parseURLTuple.printSchema()
  }

  test("testCampaignMediumHost"){
    val rawEvents: ETLStep = ETLStep("rawEvent", "read raw events",
      read.json("/tmp/1436745600000"), Seq())
    // Call rename opeator with a map of aliases, the last parameter determines
    // whether to remove or keep the original set of columns
    val df = RenameColumns.dataFrame(rawEvents.output,
      Map(
        "context.campaign.name" -> "campaign_name",
        "context.campaign.content" -> "campaign_content",
        "context.campaign.medium" -> "campaign_medium",
        "context.campaign.source" -> "campaign_source",
        "context.campaign.term" -> "campaign_term"
      ),false)
    df.registerTempTable("tem")

    val parseURLTuple = sql("""
      SELECT a.*, b.*
      from tem a LATERAL VIEW
      parse_url_tuple(a.properties.referrer, 'HOST', 'PATH', 'QUERY', 'QUERY:utm_campaign',
      'QUERY:utm_source', 'QUERY:utm_medium', 'QUERY:utm_content', 'QUERY:utm_term' ) b
      as sd_host, sd_path, sd_query, sd_campaign, sd_source_o, sd_medium_o, sd_content, sd_term
                            """)
    val csCE = CaseStatement.generateCampaignSource
    val cmCE = CaseStatement.generateCampaignMedium

    val enrichedRawEventsWithRevenueHostCampaignMediumHost = ETLStep("GenerateCampaignMediumSource",
      "Generate Medium and Source From Case Classes",
      AddColumns.dataFrame(parseURLTuple,
        Seq(csCE, cmCE)),
      Seq()
    )
    enrichedRawEventsWithRevenueHostCampaignMediumHost.dataFrame.printSchema()
  }

  test("testRenameColumns"){
    // Step 1 - read raw events
    val rawEvents: ETLStep = ETLStep("rawEvent", "read raw events",
      read.json("/tmp/1436745600000"), Seq())
      // Call rename opeator with a map of aliases, the last parameter determines
      // whether to remove or keep the original set of columns
      val df = RenameColumns.dataFrame(rawEvents.output,
              Map(
                "anonymousId" -> "user_id",
                "category" -> "category",
                "channel" -> "channel",
                "context.campaign.name" -> "campaign_name",
                "context.campaign.content" -> "campaign_content",
                "context.campaign.medium" -> "campaign_medium",
                "context.campaign.source" -> "campaign_source",
                "context.campaign.term" -> "campaign_term",
                "originalTimestamp" -> "utc_time",
                "context.ip" -> "ip_address",
                "context.page.title" -> "page_name",
                "context.page.path" -> "page_path",
                "context.page.referrer" -> "page_referrer",
                "context.page.search" -> "page_params",
                "context.page.url" -> "page_url",
                "context.userAgent" -> "user_agent",
                "messageId" -> "event_id",
                "event" -> "event_name",
                "name" -> "name",
                "projectid" -> "project_id",
                "properties.search" -> "event_param",
                "properties.path" -> "event_path",
                "properties.referrer" -> "event_referrer",
                "properties.url" -> "event_url",
                "properties.title" -> "event_title",
                "properties.product" -> "product",
                "properties.currency" -> "currency",
                "properties.orderId" -> "order_id",
                "properties.couponcode" -> "order_coupon_code",
                "properties.revenue" -> "order_revenue",
                "properties.discount" -> "order_discount",
                "properties.tax" -> "ordertax",
                "properties.shipping" -> "order_shipping",
                "properties.emailId" -> "order_email",
                "properties.name" -> "property_name",
                "properties.sessionid" -> "segment_session_id",
                "receivedAt" -> "received_at",
                "sentAt" -> "segment_sent_at",
                "timestamp" -> "timestamp",
                "type" -> "type",
                "userId" -> "segment_user_id"
              ),true)
    df.printSchema()
  }

  test("testCaseStatement"){
    val rawEvents: ETLStep = ETLStep("rawEvent", "read raw events",
      read.json("/tmp/1436745600000"), Seq())

    val enrichedRawEvents: ETLStep = ETLStep("enrichRawEvents", "embelish raw events",
        RenameColumns.dataFrame(
          FlattenArrayField.dataFrame(rawEvents.output, "properties.products", "sd_product"),
          Map(
            "sd_product_price" -> "sd_product_price",
            "sd_product_productname" -> "sd_product_name",
            "sd_product_quantity" -> "sd_product_quantity"
          ),false),
      Seq(rawEvents))

    val productCategoryCaseBranches = List( ("sd_product_name","%AcmeAntiAging%", "AntiAging"),
      ("sd_product_name","%AcmeLaserHair%", "LHR"),
      ("sd_product_name","%AcmeAcne%", "Acne"),
      ("sd_product_name","%AcmeHRL%", "HRL Precision")
    )
    val productCategoryCSElseValue = "Other"
    val productCategoryCSAttrName = "sd_product_category"
    val ce1 = CaseStatement.caseUsingLike(productCategoryCaseBranches, productCategoryCSElseValue, productCategoryCSAttrName)

    val enrichedRawEventsWithRevenue = ETLStep("addCaseStatement", "Generate New Columns From Case Classes",
      AddColumns.dataFrame(enrichedRawEvents.output, Seq(ce1)),
      Seq(enrichedRawEvents)
    )
    enrichedRawEventsWithRevenue.dataFrame.printSchema()
  }

  test("testRevenue"){
    def etlExpressions: ETLExpressions = AcmeETLExpressions

    val rawEvents: ETLStep = ETLStep("rawEvent", "read raw events",
      read.json("/tmp/1436745600000"), Seq())

    val enrichedRawEvents: ETLStep = ETLStep("enrichRawEvents", "embelish raw events",
      RenameColumns.dataFrame(
        FlattenArrayField.dataFrame(rawEvents.output, "properties.products", "sd_product"),
        Map(
          "sd_product_price" -> "sd_product_price",
          "sd_product_productname" -> "sd_product_name",
          "sd_product_quantity" -> "sd_product_quantity",
          "event" -> "event_name"
        ),false),
      Seq(rawEvents))

    // Few more case statements
    val ce2 = CaseStatement.caseUsingEquals(List(("event_name", "Completed Order", 1)), 0, IntegerType, "sd_is_revenue_session")
    val ce3 = CaseStatement.caseUsingEquals(List(("event_name", "AddToBag", 1)), 0, IntegerType, "sd_is_cart_session")
    val ce4 = CaseStatement.caseUsingEquals(List(("event_name", "Click Video", 1)), 0, IntegerType, "sd_is_video_session")

    val enrichedRawEventsWithRevenue = ETLStep("addCaseStatement", "Generate New Columns From Case Classes",
      AddColumns.dataFrame(enrichedRawEvents.output, Seq(ce2,ce3,ce4, etlExpressions.revenueExpr)),
      Seq(enrichedRawEvents)
    )
    enrichedRawEventsWithRevenue.dataFrame.printSchema()
    enrichedRawEventsWithRevenue.dataFrame.registerTempTable("tem")

    val t = sql("select sd_is_revenue_session, sd_is_cart_session, sd_is_video_session, sd_product_revenue from tem where sd_product_revenue > 0 limit 10")
    t.take(10).foreach(println)
  }

  test("testConversion"){
    val sds = read.parquet("/tmp/acme/sd_user_metrics/sd_year=2015/sd_month=7/sd_day=28")
    //val sds = read.parquet("/tmp/acme/sd_sessions/")
    sds.printSchema()
    sds.registerTempTable("sd_user_metrics")
    val df = TestHive.sql("""select campaign_name, campaign_content, campaign_medium, campaign_source, campaign_term, sd_host, sd_path, sd_campaign, sd_source, sd_medium, sd_content, sd_term, utc_time, channel, ip_address, sd_browser, sd_device_os, sd_device_family, sd_country_code, sd_country_name, sd_region, sd_city, sd_latitude, sd_longitude, sd_postal_code, sd_continent, sd_year, sd_month, sd_day, SUM(CASE WHEN sd_daily_revenue > 0 THEN 1 ELSE 0 END) as sd_rev_user, count(user_id) as sd_all_user from sd_user_metrics group by campaign_name, campaign_content, campaign_medium, campaign_source, campaign_term, sd_host, sd_path, sd_campaign, sd_source, sd_medium, sd_content, sd_term, utc_time, channel, ip_address, sd_browser, sd_device_os, sd_device_family, sd_country_code, sd_country_name, sd_region, sd_city, sd_latitude, sd_longitude, sd_postal_code, sd_continent, sd_year, sd_month, sd_day
                            """)
    println(df.count())

  }

  test("goals"){
    /*val sde = read.parquet("s3n://sparklinedata/customers/acme/cubes/sd_events/sd_year=2015/sd_month=7/sd_day=15")
    // /tmp/acme/sd_events/
    //println(sde.count())
    sde.printSchema()
    sde.registerTempTable("sd_events")
    //val df = sql("select count(*) from sd_events")
    //println(df)
    val pcr = sql("""select
                  |sd_product_category,
                  |sd_product_name,
                  |currency,
                  |order_coupon_code,
                  |sd_medium,
                  |sd_source,
                  |sd_campaign,
                  |sd_content,
                  |sd_term,
                  |sd_host,
                  |sd_path,
                  |sd_browser,
                  |sd_device_os,
                  |sd_device_family,
                  |sd_country_name,
                  |sd_region,
                  |sd_city,
                  |event_name,
                  |user_id,
                  |case when event_name ='AddToBag' then user_id  else 'NA' end as sd_bag_user,
                  |case when event_name ='Completed Order' then  user_id else 'NA' end as sd_order_user,
                  |case when event_name ='Click Video' then  user_id else 'NA' end as sd_video_user,
                  |sd_year,
                  |sd_month,
                  |sd_day,
                  |sd_product_quantity,
                  |sd_product_revenue,
                  |sd_product_price,
                  |order_discount
                  |from sd_events
                  |where sd_product_name is not null""".stripMargin)
    pcr.take(50).foreach(println) Option("yy-MM-dd HH:mm:ss.SSS")
    */
    //val sds = read.parquet("/tmp/acme/sd_sessions/sd_year=2015/sd_month=7/sd_day=28")
    val sds = read.parquet("/tmp/acme/sd_sessions/")
    sds.printSchema()
    sds.registerTempTable("sd_sessions")

    val gaolEventAttrs = Seq("user_id",
      "sd_session_id",
      "utc_time",
      "sd_product_category",
      "sd_product_name",
      "sd_year",
      "sd_month",
      "sd_day")

    val whichDay = 1438041600000L
    val impactLookback = 30
    val endDate = new DateTime(whichDay, DateTimeZone.UTC)
    val goalPeriodExpr = endDate to endDate + 1.day
    val filterGoalRowsExpr = goalPeriodExpr containsE dateTime('utc_time)

    val goalEventsStep = ETLStep("sd_goal_events",
      "Find all goal events and corresponding revenue by day by product",
        RenameColumns.dataFrame(
          AggregateColumns.dataFrame(
            FilterRows.dataFrame(
              sds,
              org.apache.spark.sql.catalyst.expressions.And(
                EqualTo(UnresolvedAttribute("event_name"), Literal("Completed Order")),
                EqualTo(filterGoalRowsExpr, Literal(true))
              )
            ),
            gaolEventAttrs,
            Seq(
              Alias(Sum(UnresolvedAttribute("sd_product_quantity")), "sd_product_quantity")(),
              Alias(Sum(UnresolvedAttribute("sd_product_revenue")), "sd_product_revenue")()
            )
          ),
          Map(
            "user_id" -> "goal_user_id",
            "utc_time"-> "sd_goal_date_time_utc"
            ),
          false
        ),
      Seq()
    )
    //goalEventsStep.dataFrame.printSchema()
    //println(goalEventsStep.dataFrame.count())
    //goalEventsStep.dataFrame.take(10).foreach(println)

    //def impactDateFilterExpr : NamedExpression = ((DateTime.now() to DateTime.now() - 30.day) contains dateTime('utc_time)))

    //val endDate = DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTime.now(DateTimeZone.UTC))

    //val periodExpr = dateTime(endDate) - 30.day to dateTime(endDate)
    val impactPeriodExpr = (endDate - (impactLookback).day) to (endDate + 1.day)
    val filterImpactRowsExpr = impactPeriodExpr containsE dateTime('utc_time)

    val impactEventAttrs = Seq("user_id",
      "page_path",
      "page_name",
      "page_url",
      "event_name",
      "sd_host",
      "sd_path",
      "sd_campaign",
      "sd_source",
      "sd_medium",
      "sd_content",
      "sd_term",
      "utc_time",
      "sd_browser",
      "sd_device_os",
      "sd_device_family",
      "sd_country_code",
      "sd_country_name",
      "sd_region",
      "sd_city",
      "sd_latitude",
      "sd_longitude",
      "sd_continent",
      "sd_postal_code",
      "sd_year",
      "sd_month",
      "sd_day"
      )

    val impactEventsStep = ETLStep("sd_impact_events",
      "Last thirty days of impact events",
      RenameColumns.dataFrame(
        SelectColumns.dataFrame(
          FilterRows.dataFrame(
            sds,
            org.apache.spark.sql.catalyst.expressions.And(
               EqualTo(filterImpactRowsExpr, Literal(true)),
                org.apache.spark.sql.catalyst.expressions.And(
                  org.apache.spark.sql.catalyst.expressions.And(
                    Not(Like(UnresolvedAttribute("page_path"), Literal("%orderConfirmation%"))),
                    Not(Like(UnresolvedAttribute("page_path"), Literal("%cart%")))
                  ),
                  org.apache.spark.sql.catalyst.expressions.And(
                    Not(Like(UnresolvedAttribute("page_path"), Literal("%cart%"))),
                    Not(Like(UnresolvedAttribute("page_path"), Literal("%payment%")))
                  )
                )
             )
          ),
          impactEventAttrs
        ),
        Map(
          "utc_time"-> "sd_impact_date_time_utc",
          "sd_year" -> "sd_impact_year",
          "sd_month" -> "sd_impact_month",
          "sd_day" -> "sd_impact_day"
        ),
        false
      ),
      Seq()
    )
    //println(impactEventsStep.dataFrame.count())

    /*
      given two ETL steps, perform a join
      input params - etl_step1, etl_step2, join type (default is Inner),
     */

    /*
    val plan = Join(
        Subquery("ie", impactEventsStep.dataFrame.queryExecution.logical),
        Subquery("ge", impactEventsStep.dataFrame.queryExecution.logical),
        Inner,
        Some(EqualTo(
          UnresolvedAttribute("ie.user_id"),
          UnresolvedAttribute("ge.goal_user_id")
          //AttributeReference("goal_user_id", StringType, false)()
        ))
    )
    //println(plan)
    val newDF = new DataFrame(impactEventsStep.dataFrame.sqlContext, plan)
    */

    //val mils = dateTime('utc_time) millis
    //val pst  = dateTimeWithTZ('utc_time, Some("yy-MM-dd HH:mm:ss.SSS")) withZone("US/Pacific")
    //$mils as utc_time_in_millis, $pst as pst_time,

    val daysBetweenExpr : NamedExpression = ((dateTimeWithTZ('sd_goal_date_time_utc) dayOfYear) -
      (dateTimeWithTZ('sd_impact_date_time_utc) dayOfYear)) as "sd_days_to_goal"

    val secondsBetweenExpr : NamedExpression = (((dateTimeWithTZ('sd_goal_date_time_utc) millis)
      - (dateTimeWithTZ('sd_impact_date_time_utc) millis)) / 1000) as "sd_seconds_to_goal"

    val goalAnalysisStep = ETLStep("sd_goals",
      "Goal Analysis With 30 days of look-back",
      AddColumns.dataFrame(
        PerformJoin.dataFrame(
          impactEventsStep.dataFrame,
          goalEventsStep.dataFrame,
          ("user_id", "goal_user_id")
        ),
        Seq(daysBetweenExpr,secondsBetweenExpr)
      ),
      Seq(),
      true,
      Seq("sd_year", "sd_month", "sd_day")
    )
    goalAnalysisStep.output.printSchema()
    println(s"final cube count: ${goalAnalysisStep.dataFrame.count()}")
    val p = goalAnalysisStep.queryExecution.logical
    println(p)

    /*
    //val projections : Seq[NamedExpression] = sds.schema.filter(f => !oldColNames.contains(f.name)).map(f => UnresolvedAttribute(f.name))
    val goalsDF =  sql("""
                      | select
                      | user_id,
                      | sd_session_id,
                      | utc_time as sd_goal_date_time_utc,
                      | sd_product_category,
                      | sd_product_name,
                      | sd_year,
                      | sd_month,
                      | sd_day,
                      | sum(sd_product_quantity) as sd_product_quantity,
                      | sum(sd_product_revenue) as sd_product_revenue
                      | from sd_sessions
                      | where event_name = 'Completed Order'
                      | group by
                      | user_id,
                      | sd_session_id,
                      | utc_time,
                      | sd_product_category,
                      | sd_product_name,
                      | sd_year,
                      | sd_month,
                      | sd_day
                    """.stripMargin)
    println(s"Goal Events count: ${goalsDF.count()}")
    goalsDF.take(10).foreach(println)
    goalsDF.registerTempTable("goals_temp")

    val impactDate = dateTimeWithTZ('utc_time)
    val goalDate = dateTimeWithTZ('sd_goal_date_time_utc)
    val filterCondI = impactDate < goalDate
    val filterCondII = impactDate > goalDate - 30.days
    val goals = sql(
      date"""
            |select
            |sd_medium,
            |sd_source,
            |sd_campaign,
            |sd_content,
            |sd_term,
            |sd_host,
            |sd_path,
            |sd_browser,
            |sd_device_os,
            |sd_device_family,
            |sd_country_name,
            |sd_region,
            |sd_city,
            |page_path,
            |page_name,
            |page_url,
            |event_name,
            |a.sd_year as sd_impact_year,
            |a.sd_month as sd_impact_month,
            |a.sd_day as sd_impact_day,
            |a.utc_time as sd_impact_date_time_utc,
            |b.sd_year as sd_goal_year,
            |b.sd_month as sd_goal_month,
            |b.sd_day as sd_goal_day,
            |sd_goal_date_time_utc,
            |$daysBetween as sd_days_to_goal,
            |$secondsBetween as sd_seconds_to_goal,
            |b.sd_product_category,
            |b.sd_product_name,
            |b.sd_product_quantity,
            |b.sd_product_revenue
            |from sd_sessions a, goals_temp b
            |where a.user_id = b.user_id
            |and $filterCondI
            |and $filterCondII
            |and (
            |    page_path not like '%orderConfirmation%' and
            |    page_path not like '%cart%' and
            |    page_path not like '%orderReview%' and
            |    page_path not like '%payment%')
          """.stripMargin)
    //var plan = goals.queryExecution.analyzed
    println(s"Final Cube Count: ${goals.count()}")
    goals.take(10).foreach(println)
    */
  }

  test("goalCube"){
    val rawDataBasePath = "/tmp/acme/sd_sessions"
    val analyticsBasePath = "/tmp/acme"
    val dayInMills = 1438128000000L

    val etlBasePath = "/tmp/acme"

    val acme = new DailyCubesWithLookbackETL(rawDataBasePath, analyticsBasePath, dayInMills)(TestHive)

    val etl = new ETLProcess(acme)
    etl.run
  }

}

/*
import com.github.nscala_time.time.Imports._

DateTime.now // returns org.joda.time.DateTime = 2009-04-27T13:25:42.659-07:00

DateTime.now.hour(2).minute(45).second(10) // returns org.joda.time.DateTime = 2009-04-27T02:45:10.313-07:00

DateTime.now + 2.months // returns org.joda.time.DateTime = 2009-06-27T13:25:59.195-07:00

DateTime.nextMonth < DateTime.now + 2.months // returns Boolean = true

DateTime.now to DateTime.tomorrow  // return org.joda.time.Interval = > 2009-04-27T13:47:14.840/2009-04-28T13:47:14.840

(DateTime.now to DateTime.nextSecond).millis // returns Long = 1000

2.hours + 45.minutes + 10.seconds
// returns com.github.nscala_time.time.DurationBuilder
// (can be used as a Duration or as a Period)

(2.hours + 45.minutes + 10.seconds).millis
// returns Long = 9910000

2.months + 3.days
// returns Period
 */
/*
select campaign_name, campaign_content, campaign_medium, campaign_source, campaign_term, sd_host, sd_path, sd_campaign, sd_source, sd_medium, sd_content, sd_term, utc_time, channel, ip_address, sd_browser, sd_device_os, sd_device_family, sd_country_code, sd_country_name, sd_region, sd_city, sd_latitude, sd_longitude, sd_postal_code sd_continent sd_year, sd_month, sd_day SUM(CASE WHEN sd_daily_revenue > 0 THEN 1 ELSE 0 END) as sd_rev_user, count(user_id) as sd_all_user from sd_user_metrics group by campaign_name, campaign_content, campaign_medium, campaign_source, campaign_term, sd_host, sd_path, sd_campaign, sd_source, sd_medium, sd_content, sd_term, utc_time, channel, ip_address, sd_browser, sd_device_os, sd_device_family, sd_country_code, sd_country_name, sd_region, sd_city, sd_latitude, sd_longitude, sd_postal_code sd_continent sd_year, sd_month, sd_day

val df = TestHive.sql("""
                        select
                               |user_id,
                               |campaign_name,
                               |campaign_content,
                               |campaign_medium,
                               |campaign_source,
                               |campaign_term,
                               |sd_host,
                               |sd_path,
                               |sd_campaign,
                               |sd_source,
                               |sd_medium,
                               |sd_content,
                               |sd_term,
                               |utc_time,
                               |channel,
                               |ip_address,
                               |sd_browser,
                               |sd_device_os,
                               |sd_device_family,
                               |sd_country_code,
                               |sd_country_name,
                               |sd_region,
                               |sd_city,
                               |sd_latitude,
                               |sd_longitude,
                               |sd_postal_code,
                               |sd_year,
                               |sd_month,
                               |sd_day,
                               |sd_continent,
                               |sum(sd_rev_user) sd_paying_users,
                               |sum(sd_all_users) as sd_all_users
                               |from
                               |( select
                               |user_id,
                               |campaign_name,
                               |campaign_content,
                               |campaign_medium,
                               |campaign_source,
                               |campaign_term,
                               |sd_host,
                               |sd_path,
                               |sd_campaign,
                               |sd_source,
                               |sd_medium,
                               |sd_content,
                               |sd_term,
                               |utc_time,
                               |channel,
                               |ip_address,
                               |sd_browser,
                               |sd_device_os,
                               |sd_device_family,
                               |sd_country_code,
                               |sd_country_name,
                               |sd_region,
                               |sd_city,
                               |sd_latitude,
                               |sd_longitude,
                               |sd_postal_code,
                               |sd_year,
                               |sd_month,
                               |sd_day,
                               |sd_continent,
                               |case when sum(sd_daily_revenue) > 0 then count( distinct user_id)  else 0 end  as sd_rev_user,
                               |count( distinct user_id)  as sd_all_users
                               |from sd_user_metrics
                               |group by
                               |user_id,
                               |campaign_name,
                               |campaign_content,
                               |campaign_medium,
                               |campaign_source,
                               |campaign_term,
                               |sd_host,
                               |sd_path,
                               |sd_campaign,
                               |sd_source,
                               |sd_medium,
                               |sd_content,
                               |sd_term,
                               |utc_time,
                               |channel,
                               |ip_address,
                               |sd_browser,
                               |sd_device_os,
                               |sd_device_family,
                               |sd_country_code,
                               |sd_country_name,
                               |sd_region,
                               |sd_city,
                               |sd_latitude,
                               |sd_longitude,
                               |sd_postal_code,
                               |sd_year,
                               |sd_month,
                               |sd_day,
                               |sd_continent
                               |) a
                               |group by
                               |user_id,
                               |campaign_name,
                               |campaign_content,
                               |campaign_medium,
                               |campaign_source,
                               |campaign_term,
                               |sd_host,
                               |sd_path,
                               |sd_campaign,
                               |sd_source,
                               |sd_medium,
                               |sd_content,
                               |sd_term,
                               |utc_time,
                               |channel,
                               |ip_address,
                               |sd_browser,
                               |sd_device_os,
                               |sd_device_family,
                               |sd_country_code,
                               |sd_country_name,
                               |sd_region,
                               |sd_city,
                               |sd_latitude,
                               |sd_longitude,
                               |sd_postal_code,
                               |sd_year,
                               |sd_month,
                               |sd_day,
                               |sd_continent
                               """.stripMargin)
   println(df.take(50).foreach(println))
   */
/*
    val productCube = TestHive.sql(
      """select user_id,
         campaign_name,
         campaign_content,
         campaign_medium,
         campaign_source,
         campaign_term,
         sd_host,
         sd_path,
         sd_campaign,
         sd_source,
         sd_medium,
         sd_content,
         sd_term,
         utc_time,
         channel,
         ip_address,
         sd_browser,
         sd_device_os,
         sd_device_family,
         sd_country_code,
         sd_country_name,
         sd_region,
         sd_city,
         sd_latitude,
         sd_longitude,
         sd_postal_code,
         sd_continent,
         sd_year,
         sd_month,
         sd_day,
         sum(sd_rev_user) sd_paying_users,
         sum(sd_all_users) as sd_all_users
         from ( case when sum(sd_daily_revenue) > 0 then count( distinct user_id)  else 0 end  as sd_rev_user, count( distinct user_id)  as sd_all_users from sd_user_metrics group by user_id, campaign_name, campaign_content, campaign_medium, campaign_source, campaign_term, sd_host, sd_path, sd_campaign, sd_source, sd_medium, sd_content, sd_term, utc_time, channel, ip_address, sd_browser, sd_device_os, sd_device_family, sd_country_code, sd_country_name, sd_region, sd_city, sd_latitude, sd_longitude, sd_postal_code, sd_year, sd_month, sd_day, sd_continent ) a group by user_id, campaign_name, campaign_content, campaign_medium, campaign_source, campaign_term, sd_host, sd_path, sd_campaign, sd_source, sd_medium, sd_content, sd_term, utc_time, channel, ip_address, sd_browser, sd_device_os, sd_device_family, sd_country_code, sd_country_name, sd_region, sd_city, sd_latitude, sd_longitude, sd_postal_code, sd_year, sd_month, sd_day, sd_continent
              """.stripMargin)
              */
//var endTime = org.joda.time.DateTime.now()
//var seconds = Seconds.secondsBetween(startTime, endTime).getSeconds()
//println(s"Time taken: $seconds")

//var plan1 = gDF.queryExecution.logical
//val gDF : DataFrame = sql("select distinct sd_session_id from sd_sessions")
//gDF.take(100).foreach(println)