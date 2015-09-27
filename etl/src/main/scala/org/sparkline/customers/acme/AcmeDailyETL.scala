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
 * Created by Jitender on 7/29/15.
 */

 package org.sparkline.customers.acme
import org.apache.hadoop.hive.ql.exec.FunctionRegistry
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedExtractValue, UnresolvedFunction}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.{LeftOuter, JoinType}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.DataFrame

import org.joda.time.{DateTimeZone, DateTime}
import com.github.nscala_time.time.Imports._

import org.sparkline.etl.operators._
import org.sparklinedata.spark.dateTime.Functions
import org.apache.spark.sql.catalyst.dsl.expressions._

import scala.collection.mutable.ListBuffer
import org.sparkline.etl._
import org.sparklinedata.spark.dateTime.dsl.expressions._


object AcmeETLExpressions extends ETLExpressions {
  val timeBetweenEventsForSessionTagging : Integer = 30
  val numberOfTiles : Integer = 5
  val bounceThreshold : Integer = 3 //tag a session as bounce session if session duration is less than this threshold

  val toDateTimeFunc : String = "spklDateTme"
  //val flattenFieldMap : Map[String, String] = Map("properties.products", "sd_product")
  val flattenFieldAttr : String = "properties.products"
  val flattenFieldPrefix : String = "sd_product"

  val rawEventFieldAliases : Map[String, String] = Map(
    "anonymousId" -> "user_id",
    //"category" -> "category",
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
    //"name" -> "name",
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
    //"properties.shipping" -> "order_shipping",
    //"properties.emailId" -> "order_email",
    //"properties.name" -> "property_name",
    //"properties.sessionid" -> "segment_session_id",
    "receivedAt" -> "received_at",
    "sentAt" -> "segment_sent_at",
    "timestamp" -> "utc_time",
    "type" -> "type",
    "userId" -> "segment_user_id",
    "sd_product_price" -> "sd_product_price",
    "sd_product_productname" -> "sd_product_name",
    "sd_product_quantity" -> "sd_product_quantity"
  )

  val transformedEventFieldAliases : Map[String, String] = Map(
    "sd_user_agent.family" -> "sd_browser",
    "sd_user_agent.major" -> "sd_browser_version_major",
    "sd_user_agent.minor" -> "sd_browser_version_minor",
    "sd_user_agent.patch" -> "sd_browser_version_patch",
    "sd_user_agent.os.family" ->  "sd_device_os",
    "sd_user_agent.os.major" ->  "sd_device_os_version_major",
    "sd_user_agent.os.minor" ->  "sd_device_version_minor",
    "sd_user_agent.os.patch" -> "sd_device_os_version_patch",
    "sd_user_agent.os.patchMinor" -> "sd_device_os_patch_minor",
    "sd_user_agent.device.family" ->  "sd_device_family",
    "sd_ip_address.countryCode" -> "sd_country_code",
    "sd_ip_address.countryName" -> "sd_country_name",
    "sd_ip_address.region" -> "sd_region",
    "sd_ip_address.city" -> "sd_city",
    "sd_ip_address.geoPoint.latitude" -> "sd_latitude",
    "sd_ip_address.geoPoint.longitude" -> "sd_longitude",
    "sd_ip_address.postalCode" -> "sd_postal_code",
    "sd_ip_address.continent" -> "sd_continent"
  )

  val dropEventFields : Seq[String] = Seq("integrations","original_timestamp")

  //def toFormatDateFuncName : String = "spklFormatDate"
  //def toDateFormat : String  = "YYYY-MM-dd"

  override def pagePathAttr : String = "page_path"
  override def pageNameAttr : String = "page_name"
  override def pageReferrerAttr : String = "page_referrer"
  override def eventNameAttr : String = "event_name"
  override def timestampAttr : String = "utc_time"
  override def anonymousCookieAttr : String = "user_id"
  override def userAgentAttr : String = "user_agent"
  override def ipAttr : String = "ip_address"

  /*
  sessionization related
  */
  override def sessionMatchExpression : Predicate = (
    toDateTime(UnresolvedAttribute(timestampAttr)) - toDateTime(UnresolvedAttribute(prevEventTimeAttr))) > Literal(timeBetweenEventsForSessionTagging)

  override def sessionIdExpression : Expression = UnresolvedFunction("concat", Seq(UnresolvedAttribute(anonymousCookieAttr), Literal(":"), UnresolvedAttribute(timestampAttr)))
  /*
    // throws null pointer exception since prevEvntTimeAttrName has null values
    val sessionMatchExpression : Predicate = (
    toDateTimeInMillis(timestampAttr) - toDateTimeInMillis(UnresolvedAttribute(prevEvntTimeAttrName))) > Literal(30)
  */

  /*
  revenue releated
  */

  val revenueEventName : String = "Completed Order"
  override def revenueEquation = Multiply(UnresolvedAttribute(productQuantityAttr), UnresolvedAttribute(productPriceAttr))
  val revenueCondition =  Seq(EqualTo(UnresolvedAttribute(eventNameAttr), Literal(revenueEventName)), revenueEquation, Literal(0))
  override def revenueExpr = Alias(CaseWhen(revenueCondition), productRevenueAttr)()

  // Field for user metric attributes
  val userMetricsAttrs = Seq("user_id",
    "campaign_name",
    "campaign_content",
    "campaign_medium",
    "campaign_source",
    "campaign_term",
    "sd_host",
    "sd_path",
    "sd_campaign",
    "sd_source",
    "sd_medium",
    "sd_content",
    "sd_term",
    "utc_time",
    "channel",
    "ip_address",
    "sd_browser",
    "sd_device_os",
    "sd_device_family",
    "sd_country_code",
    "sd_country_name",
    "sd_region",
    "sd_city",
    "sd_latitude",
    "sd_longitude",
    "sd_postal_code",
    "sd_year",
    "sd_month",
    "sd_day",
    "sd_continent"
  )

  /*
  * Attributes for building goals cube
  */
  val impactLookbackDays : Integer = 30
  val impactEventNames = Set[Any]("Click Video", "Click Slider")
  val excludeFromImpactEvents = Set[Any]("AddToBag", "Completed Order", null)
  val goalEventName = "Completed Order"

  val excludeFromImpactPages = Set[Any](
    "/replenish.htm",
    "/forgotPassword.htm",
    "/orderConfirmation.htm",
    "/changeEmail.htm",
    "/at-home-permanent-laser-hair-removal-system-print.htm",
    "/activation-precision-confirmation.htm",
    "/cart.htm",
    "/payment.htm",
    "/orderReview.htm",
    "/changePassword.htm",
    "/addresBook.htm",
    "/login.htm",
    "/confirmation.htm",
    "/orderHistory.htm",
    null
  )
  val impactEventTimeAttrName = "sd_impact_date_time_utc"
  val impactSessionIdAttrName =  "sd_impact_session_id"
  val impactSessionIdAttrExpr = UnresolvedAttribute(impactSessionIdAttrName)
  val impactSessionDurationAttrExpr = UnresolvedAttribute(sessionDurationAttr)

  val impactEventAttrs = Seq(
    "user_id",
    "sd_session_id",
    "utc_time",
    "page_path",
    "event_name",
    "sd_host",
    "sd_path",
    "sd_campaign",
    "sd_source",
    "sd_medium",
    "sd_content",
    "sd_term",
    "sd_browser",
    "sd_device_os",
    "sd_device_family",
    "sd_country_code",
    "sd_city",
    "sd_year",
    "sd_month",
    "sd_day"
  )

  val impactEventAttrsAliases =  Map(
    "user_id" -> "sd_impact_user_id",
    "sd_session_id" -> impactSessionIdAttrName,
    "utc_time"-> impactEventTimeAttrName,
    "page_path" -> "sd_impact_page_path",
    "event_name" -> "sd_impact_event_name",
    "sd_host" -> "sd_impact_host",
    "sd_path" -> "sd_impact_path",
    "sd_campaign"  -> "sd_impact_campaign",
    "sd_source" -> "sd_impact_source",
    "sd_medium" -> "sd_impact_medium",
    "sd_content"  -> "sd_impact_content",
    "sd_term" -> "sd_impact_term",
    "sd_browser" -> "sd_impact_browser",
    "sd_device_os" -> "sd_impact_device_os",
    "sd_device_family" -> "sd_impact_device_family",
    "sd_country_code" -> "sd_impact_country_code",
    "sd_city" -> "sd_impact_city",
    "sd_year" -> "sd_impact_year",
    "sd_month" -> "sd_impact_month",
    "sd_day" -> "sd_impact_day"
  )

  val goalEventTimeAttrName = "sd_goal_date_time_utc"
  val goalSessionIdAttrName =  "sd_goal_session_id"
  val goalSessionIdAttrExpr = UnresolvedAttribute(goalSessionIdAttrName)

  val goalEventAttrs = Seq(
    "user_id",
    "sd_session_id",
    "utc_time",
    "sd_product_category",
    "sd_product_name",
    "sd_host",
    "sd_path",
    "sd_campaign",
    "sd_source",
    "sd_medium",
    "sd_content",
    "sd_term",
    "sd_browser",
    "sd_device_os",
    "sd_device_family",
    "sd_country_code",
    "sd_city",
    "sd_year",
    "sd_month",
    "sd_day"
  )

  val goalEventAttrsAliases = Map(
    "user_id" -> "sd_goal_user_id",
    "sd_session_id" ->goalSessionIdAttrName,
    "utc_time"-> goalEventTimeAttrName,
    "sd_product_category" -> "sd_goal_product_category",
    "sd_product_name" -> "sd_goal_product_name",
    "sd_host" -> "sd_goal_host",
    "sd_path" -> "sd_goal_path",
    "sd_campaign"  -> "sd_goal_campaign",
    "sd_source" -> "sd_goal_source",
    "sd_medium" -> "sd_goal_medium",
    "sd_content"  -> "sd_goal_content",
    "sd_term" -> "sd_goal_term",
    "sd_browser" -> "sd_goal_browser",
    "sd_device_os" -> "sd_goal_device_os",
    "sd_device_family" -> "sd_goal_device_family",
    "sd_country_code" -> "sd_goal_country_code",
    "sd_city" -> "sd_goal_city",
    productQuantityAttr -> productQuantityAttr,
    productRevenueAttr -> productRevenueAttr,
    "sd_year" -> "sd_year",
    "sd_month" -> "sd_month",
    "sd_day" -> "sd_day"
  )

  override val impactEventTimeExpr = dateTime(UnresolvedAttribute(impactEventTimeAttrName))
  override val goalEventTimeExpr = dateTime(UnresolvedAttribute(goalEventTimeAttrName))

  val goalJoinOnCols = ("sd_goal_user_id", "sd_impact_user_id")
  val sessionIdJoinOnCols = ("sd_impact_session_id", "sd_session_id")

  override val goalEventTypeFilterExpr  : Expression =
    EqualTo(UnresolvedAttribute(eventNameAttr), Literal(goalEventName))

  override val productQuantityByGoalExpr : NamedExpression =
    Alias(Sum(UnresolvedAttribute(productQuantityAttr)), productQuantityAttr)()

  val productRevenueAttrExpr = UnresolvedAttribute(productRevenueAttr)
  override val productRevenueByGoalExpr : NamedExpression =
    Alias(Sum(productRevenueAttrExpr), productRevenueAttr)()

  override def goalEventAggrExpr = Seq(productQuantityByGoalExpr, productRevenueByGoalExpr)

  // all goal metrics are defined here
  val daysBetweenExpr : NamedExpression = Alias((goalEventTimeExpr dayOfYear) -
    (impactEventTimeExpr dayOfYear), "sd_days_to_goal")()

  val secondsBetweenExpr : NamedExpression = Alias(
    ((goalEventTimeExpr millis) - (impactEventTimeExpr millis)) / 1000,
    "sd_seconds_to_goal")()

  val sessionsToGoalFunction =  UnresolvedWindowFunction("count", Seq(impactSessionIdAttrExpr))
  val timeSpentToGoalFunction =  UnresolvedWindowFunction("sum", Seq(impactSessionDurationAttrExpr))
  val revAttrToImpactEventFunction = UnresolvedWindowFunction("count", Seq(productRevenueAttrExpr))
  val impactEventGroupRankFunction = UnresolvedWindowFunction("rank", Seq())

  val partitionByGoalSessionExpr : Seq[Expression] = Seq(goalSessionIdAttrExpr)
  val sortByImpactEventTimestamp : Seq[SortOrder] = Seq(SortOrder(UnresolvedAttribute(impactEventTimeAttrName), Ascending))

  val windowFrame = SpecifiedWindowFrame(RowFrame, CurrentRow, UnboundedFollowing)
  //rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING
  val windowFrameII = SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing)
  val wdwSpec = WindowSpecDefinition(partitionByGoalSessionExpr, sortByImpactEventTimestamp, windowFrame)
  val wdwSpecII = WindowSpecDefinition(partitionByGoalSessionExpr, sortByImpactEventTimestamp, windowFrameII)
  val wdwSpecIII = WindowSpecDefinition(partitionByGoalSessionExpr, sortByImpactEventTimestamp, UnspecifiedFrame)

  val sessionsToGoalExpr : NamedExpression = Alias(Subtract(WindowExpression(sessionsToGoalFunction, wdwSpec), Literal(1)),
    "sd_sessions_to_goal")()
  val timeSpentToGoalExpr : NamedExpression = Alias(Subtract(WindowExpression(timeSpentToGoalFunction, wdwSpec), impactSessionDurationAttrExpr),
    "sd_time_to_goal")()
  val multiTouchRevAttrToImpactEventExpr : NamedExpression = Alias(Divide(productRevenueAttrExpr,
    WindowExpression(revAttrToImpactEventFunction, wdwSpecII)), "sd_multi_touch_attr_revenue")()
  val impactEventGroupRankExpr : NamedExpression = Alias(WindowExpression(impactEventGroupRankFunction, wdwSpecIII),
    "impact_event_group_rank")()

  val goalMetrics = Seq(daysBetweenExpr, sessionsToGoalExpr, timeSpentToGoalExpr, multiTouchRevAttrToImpactEventExpr, impactEventGroupRankExpr)
}

class DailyCubesETL(val rawDataBasePath: String = "/tmp/acme/raw/{1437264000000,1437177600000}/*.gz",
                    val analyticsBasePath: String = "/tmp/acmeOut" )
                   (implicit spklContext: HiveContext) extends ETLInfo {

  def etlExpressions: ETLExpressions = AcmeETLExpressions

  // Step 1 - read raw events, do not persist to a cube
  val rawEventsStep: ETLStep = ETLStep("rawEvent", "read raw events",
    spklContext.read.json(rawDataBasePath), Seq())

  // Step 2-I Multiple enrichment steps - Flatten product array, rename columns, remove bad fields,
  val enrichedRawEventsStepI: ETLStep = ETLStep("EnrichRawEvents",
    """
          enrich raw events - flatten product array, map columns,add new columns based on user-agent, ip and date
          and drop unwanted columns
    """,
    DropColumns.dataFrame(
      RenameColumns.dataFrame(
        AddColumns.dataFrame(
          RenameColumns.dataFrame(
            FlattenArrayField.dataFrame(rawEventsStep.output, AcmeETLExpressions.flattenFieldAttr, AcmeETLExpressions.flattenFieldPrefix),
            etlExpressions.rawEventFieldAliases,
            true
          ),
          Seq(etlExpressions.parseUserAgentExpr, etlExpressions.parseIpExpr,
            //etlExpressions.parseSparkDateExpr, etlExpressions.parseSparkDatePSTExpr,
            etlExpressions.parseYearExpr, etlExpressions.parseMonthExpr, etlExpressions.parseDayExpr,
            etlExpressions.parseDayNameExpr, etlExpressions.parseHourOfDayExpr)
        ),
        etlExpressions.transformedEventFieldAliases,
        false
      ),
      etlExpressions.dropEventFields ++ Seq(etlExpressions.eIpAttr, etlExpressions.eUserAgentAttr)
    ),
    Seq(rawEventsStep)
  )


  // Step 2 - II Adding a bunch of case statements and hive function to parse URL
  val tempTableName = "sd_temp_table"
  val sqlQuery = s"""
          SELECT a.*, b.*
          from $tempTableName a LATERAL VIEW
          parse_url_tuple(a.event_referrer, 'HOST', 'PATH', 'QUERY', 'QUERY:utm_campaign',
          'QUERY:utm_source', 'QUERY:utm_medium', 'QUERY:utm_content', 'QUERY:utm_term' ) b
          as sd_host, sd_path, sd_query, sd_campaign, sd_source_o, sd_medium_o, sd_content, sd_term
        """

  /* val productCategoryCaseBranches = List((etlExpressions.productNameAttr, "%AcmeAntiAging%", "AntiAging"),
    (etlExpressions.productNameAttr, "%AcmeLaserHair%", "LHR"),
    (etlExpressions.productNameAttr, "%AcmeAcne%", "Acne"),
    (etlExpressions.productNameAttr, "%AcmeHRL%", "HRL Precision") */

  val productCategoryCaseBranches = List((etlExpressions.productNameAttr, "%acmecharge%", "Charge"),
    (etlExpressions.productNameAttr, "%acmesurge%", "Surge"),
    (etlExpressions.productNameAttr, "%acmeflex%", "Flex"),
    (etlExpressions.productNameAttr, "%acmezip%", "Zip")
  )
  val productCategoryCSElseValue = "Aria"
  val productCategoryCSAttrName = "sd_product_category"
  val ce1 = CaseStatement.caseUsingLike(productCategoryCaseBranches, productCategoryCSElseValue, productCategoryCSAttrName)
  val ce2 = CaseStatement.caseUsingEquals(List((etlExpressions.eventNameAttr, "Completed Order", 1)), 0, IntegerType, "sd_is_revenue_event")
  val ce3 = CaseStatement.caseUsingEquals(List((etlExpressions.eventNameAttr, "AddToBag", 1)), 0, IntegerType, "sd_is_cart_event")
  val ce4 = CaseStatement.caseUsingEquals(List((etlExpressions.eventNameAttr, "Click Video", 1)), 0, IntegerType, "sd_is_video_event")
  val csCE = CaseStatement.generateCampaignSource
  val cmCE = CaseStatement.generateCampaignMedium

  val enrichedRawEventsStep = ETLStep("sd_events",
    """ Generate new columns from URL and product type using Case classes for Campaigns, Source and Medium
          for  product category
    """,
    Lag.lagOnTimeStamp(//add in previous event timestamp
      AddColumns.dataFrame(// case statements
        SQLAddColumns.dataFrame(// run sql query to add new columns using parse_url_tuple
          enrichedRawEventsStepI.dataFrame, sqlQuery, tempTableName
        ),
        Seq(ce1, ce2, ce3, ce4, etlExpressions.revenueExpr, csCE, cmCE)
      ),
      etlExpressions, 1
    ),
    Seq(enrichedRawEventsStepI),
    true,
    Seq("sd_year", "sd_month", "sd_day")
  )

  // Step 3 - Sessionize Events
  val sessionizedEventsStep = ETLStep("sd_sessions",
    "Sessionize Event Stream",
    Sessionize.dataFrame(
      enrichedRawEventsStep.output,
      etlExpressions
    ),
    Seq(enrichedRawEventsStep),
    true,
    Seq("sd_year", "sd_month", "sd_day")
  )

  /*
        // some test code
    sessionizedEventsStep.dataFrame.registerTempTable("sd_sessions")
    val g : DataFrame = spklContext.sql("select count(distinct sd_session_id) from sd_sessions")
    g.take(1).foreach(println)
    println( sessionizedEventsStep.dataFrame.count())
  */

  // Step 4 - Create Session Metrics
  val sessionDailyMetricsStep = ETLStep("sd_session_metrics",
    "Create Daily Session Metrics From Sessionized Event Stream",
    FilterRows.dataFrame(
      AddColumns.dataFrame(
        sessionizedEventsStep.output,
        Seq(etlExpressions.sessionRowNumberByTimeExpr,
          etlExpressions.sessionStartTimeExpr,
          etlExpressions.sessionEndTimeExpr,
          etlExpressions.sessionEventCountExpr,
          etlExpressions.sessionLandingScreenExpr,
          etlExpressions.sessionExitScreenExpr,
          //etlExpressions.sessionPagePathExpr,
          //etlExpressions.sessionPagesExpr,
          //etlExpressions.sessionEventPathExpr,
          etlExpressions.sessionPageReferrerExpr,
          etlExpressions.sessionProductsExpr,
          etlExpressions.sessionProductQuantityExpr,
          etlExpressions.sessionRevenueExpr,
          etlExpressions.isRevSessionExpr,
          etlExpressions.isCartSessionExpr,
          etlExpressions.isVideoSessionExpr,
          etlExpressions.sessionDurationExpr,
          etlExpressions.isBounceSessionExpr,
          etlExpressions.avgTimePerEventExpr
        )
      ),
      etlExpressions.filterSessionRows
    ),
    Seq(sessionizedEventsStep),
    true,
    Seq("sd_year", "sd_month", "sd_day")
  )

  // Step 5 - Create User Metrics
  val userDailyMetricsStep = ETLStep("sd_user_metrics",
    "Create User Daily Metrics From Session Metrics Cube",
    AddColumns.dataFrame(
      AggregateColumns.dataFrame(
        sessionDailyMetricsStep.output,
        etlExpressions.userMetricsAttrs,
        Seq(
          etlExpressions.dailyRevenueExpr,
          etlExpressions.dailyProductQuantityExpr,
          etlExpressions.dailySessionCountExpr,
          etlExpressions.dailySpentExpr,
          etlExpressions.avgSessionTimeExpr,
          etlExpressions.dailyEventCountExpr,
          etlExpressions.avgNumEventsPerSessionExpr,
          etlExpressions.lastSeenAtExpr,
          etlExpressions.firstSeenAtExpr,
          etlExpressions.dailyRevenueSessionCountExpr,
          etlExpressions.dailyCartSessionCountExpr,
          etlExpressions.dailyVideoSessionCountExpr
        )
      ),
      Seq(
        etlExpressions.ntileDailyEventCountEpr,
        etlExpressions.ntileDailySessionCountEpr,
        etlExpressions.ntileDailyTimeSpentEpr)
    ),
    Seq(sessionDailyMetricsStep),
    true,
    Seq("sd_year", "sd_month", "sd_day")
  )

  override def steps: Seq[ETLStep] = Seq(
    enrichedRawEventsStep,
    sessionizedEventsStep,
    sessionDailyMetricsStep,userDailyMetricsStep
  )
}

/*
* ETL for daily cubes that have lookback
*/

class DailyCubesWithLookbackETL(val rawDataBasePath: String = "/tmp/acme/sd_sessions",
                                val analyticsBasePath: String = "/tmp/acme", val whichDay : Long )(
                                 implicit spklContext: HiveContext) extends ETLInfo {

  def etlExpressions: ETLExpressions = AcmeETLExpressions

  def rawEventsStep: ETLStep = ???

  def enrichedRawEventsStep : ETLStep = ???

  def sessionizedEventsStep: ETLStep = ???

  def sessionDailyMetricsStep : ETLStep = ???

  def userDailyMetricsStep : ETLStep = ???

  val endDate = new DateTime(whichDay, DateTimeZone.UTC)

  // goal period -  begining of the day in mid-night to the next-day midnight
  val goalEventsPeriod = endDate to endDate + 1.day
  val goalEventsInPeriod = goalEventsPeriod containsE etlExpressions.toDateTimeExpr
  val goalEventInPeriodExpr = EqualTo(goalEventsInPeriod, Literal(true))

  // impact period - from end of day in mid-night to the look-back period
  val impactEventsPeriod = (endDate - Period.days(etlExpressions.impactLookbackDays)) to (endDate + 1.day)
  val impactEventsInPeriod = impactEventsPeriod containsE etlExpressions.toDateTimeExpr
  val impactEventsInPeriodExpr = EqualTo(impactEventsInPeriod, Literal(true))

  // Step 1 - read sessions data structure
  val sessionsDataStep = ETLStep("sd_sessions",
    "Filter to last N days data",
    spklContext.read.parquet(rawDataBasePath),
    Seq()
  )

  // Step 2 - read goal events for the given day
  // Aggregate filter - goal events in the period and the type of goal event
  val filterGoalEventRowsExpr = org.apache.spark.sql.catalyst.expressions.And(
    goalEventInPeriodExpr,
    etlExpressions.goalEventTypeFilterExpr
  )
  // goal events ETL Step
  val goalEventsStep = ETLStep("sd_goal_events",
    "Find all goal events and corresponding revenue by day by product",
    RenameColumns.dataFrame(
      AggregateColumns.dataFrame(
        DropDuplicateRows.dataFrame(
          FilterRows.dataFrame(
            sessionsDataStep.dataFrame,
            filterGoalEventRowsExpr
          ),
          Seq(etlExpressions.timestampAttr, etlExpressions.anonymousCookieAttr) //utc_time and user_id
        ),
        etlExpressions.goalEventAttrs,
        etlExpressions.goalEventAggrExpr
      ),
      etlExpressions.goalEventAttrsAliases,
      true
    ),
    Seq()
  )
  /*
   val impactPageTypeFilterExpr = org.apache.spark.sql.catalyst.expressions.And(
     org.apache.spark.sql.catalyst.expressions.And(
       Not(Like(UnresolvedAttribute(etlExpressions.pagePathAttr), Literal("%orderConfirmation%"))),
       Not(Like(UnresolvedAttribute(etlExpressions.pagePathAttr), Literal("%cart%")))
     ),
     org.apache.spark.sql.catalyst.expressions.And(
       Not(Like(UnresolvedAttribute(etlExpressions.pagePathAttr), Literal("%orderReview%"))),
       Not(Like(UnresolvedAttribute(etlExpressions.pagePathAttr), Literal("%payment%")))
     )
   )*/

  // exclude following impact events
  val impactEventTypeFilterExpr = Not(InSet(UnresolvedAttribute(etlExpressions.eventNameAttr),
    etlExpressions.excludeFromImpactEvents))

  // exclude following impact pages
  val impactPageTypeFilterExpr = Not(InSet(UnresolvedAttribute(etlExpressions.pagePathAttr),
    etlExpressions.excludeFromImpactPages))

  //overall filter expression
  val filterImpactEventExpr = org.apache.spark.sql.catalyst.expressions.And(
    impactEventsInPeriodExpr,
    org.apache.spark.sql.catalyst.expressions.And(
      impactEventTypeFilterExpr,
      impactPageTypeFilterExpr
    )
  )
  // impact event step
  val impactEventsStep = ETLStep("sd_impact_events",
    "Last n days of impact events",
    RenameColumns.dataFrame(
      //SelectColumns.dataFrame(
      DropDuplicateRows.dataFrame(
        FilterRows.dataFrame(
          sessionsDataStep.dataFrame,
          filterImpactEventExpr
        ),
        Seq(etlExpressions.timestampAttr, etlExpressions.anonymousCookieAttr) //utc_time and user_id
      ),
      //etlExpressions.impactEventAttrs
      //),
      etlExpressions.impactEventAttrsAliases,
      true
    ),
    Seq()
  )

  // Fourth Step - join two tables to generate goals cube
  val impactBeforeGoalExpr = etlExpressions.impactEventTimeExpr < etlExpressions.goalEventTimeExpr

  val joinExpression = And(
    EqualTo(UnresolvedAttribute(etlExpressions.goalJoinOnCols._1),
      UnresolvedAttribute(etlExpressions.goalJoinOnCols._2)),
    EqualTo(impactBeforeGoalExpr, Literal(true))
  )

  val goalAnalysisStepPre = ETLStep("sd_goals_pre",
    "Goal Analysis With N days of look-back",
    PerformJoin.dataFrameEx(
      impactEventsStep.dataFrame, "i1",
      goalEventsStep.dataFrame, "i2",
      joinExpression
    ),
    Seq()
  )

  //Final step  - join goals with session metrics to get session duration

  //read session metrics for session duration
  val sessionMetricsDataStep = ETLStep("sd_session_metrics",
    "Filter to last N days data",
    SelectColumns.dataFrame(
      FilterRows.dataFrame(
        spklContext.read.parquet(analyticsBasePath + "/sd_session_metrics"),
        impactEventsInPeriodExpr),
      Seq(etlExpressions.sessionIdAttr, etlExpressions.sessionDurationAttr)
    ),
    Seq()
  )

  val joinExpressionII = EqualTo(
    UnresolvedAttribute(etlExpressions.sessionIdJoinOnCols._1),
    UnresolvedAttribute(etlExpressions.sessionIdJoinOnCols._2))

  //val selectColNames = goalAnalysisStepPre.output.columns.toList :+ "sd_session_duration"
  val goalAnalysisStep = ETLStep("sd_goals",
    "Goal Analysis With N days of look-back and session duration",
    AddColumns.dataFrame(
      PerformJoin.dataFrameEx(
        goalAnalysisStepPre.output, "j1",
        sessionMetricsDataStep.dataFrame, "j2",
        joinExpressionII
      ),
      etlExpressions.goalMetrics
    ),
    Seq(),
    true,
    Seq("sd_year", "sd_month", "sd_day")
  )

  //println(s"Final goals cube count: ${goalAnalysisStep.dataFrame.count()}")
  override def steps: Seq[ETLStep] = Seq(
    goalAnalysisStep
  )

}
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