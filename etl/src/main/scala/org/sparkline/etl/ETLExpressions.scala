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
 * Co-authored by Harish.
 * Co-authored by Jitender on 7/14/15.
 */

 package org.sparkline.etl

import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.hive.HiveGenericUdtf
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.catalyst.dsl.expressions._

import org.sparkline.etl.operators.CaseStatement
import org.sparkline.utils.ExprBuilder
import org.sparklinedata.spark.dateTime.dsl.expressions._


trait PageExpressions {
  def pagePath : Expression
  def pageTitle : Expression
  def pageReferrer : Expression
  def eventPath : Expression
}

trait BaseExpressions extends ExprBuilder with PageExpressions {
  def timeBetweenEventsForSessionTagging : Integer
  def numberOfTiles : Integer
  def rawEventFieldAliases : Map[String, String]
  def transformedEventFieldAliases : Map[String, String]
  def dropEventFields : Seq[String]
  def userMetricsAttrs : Seq[String]


  def eventNameAttr : String = "event_name"
  def pagePathAttr : String = "page_path"
  def pageNameAttr : String = "page_name"
  def pageReferrerAttr : String = "page_referrer"
  def timestampAttr : String = "utc_time"
  def anonymousCookieAttr : String = "user_id"
  def userAgentAttr : String = "user_agent"
  def ipAttr : String = "ip_address"
  def revenueEventName : String


  def prevEventTimeAttr : String = "sd_previous_event_timestamp"
  def productNameAttr: String = "sd_product_name"
  def productQuantityAttr: String = "sd_product_quantity"
  def productPriceAttr: String = "sd_product_price"
  def productRevenueAttr : String = "sd_product_revenue"
  def isRevEventAttr : String = "sd_is_revenue_event"
  def isCartEventAttr : String = "sd_is_cart_event"
  def isVideoEventAttr : String = "sd_is_video_event"
  def eUserAgentAttr : String = "sd_user_agent"
  def eIpAttr : String = "sd_ip_address"

  // date related
  def yearAttr : String = "sd_year"
  def monthAttr : String = "sd_month"
  def dayAttr : String = "sd_day"
  def dayNameAttr : String = "sd_day_name"
  def hourAttr : String = "sd_hour_of_day"

  def toDateTimeFunc : String
  def toDateTime(e : Expression) = UnresolvedFunction(toDateTimeFunc, Seq(e))
  /*
  def toDateFormat : String
  def toFormatDateFuncName : String
  def dateFormatAttr = UnresolvedAttribute(toDateFormat)
  */
  def unresolvedTimestampAttr = UnresolvedAttribute(timestampAttr)
  def toDateTimeInMillis(e : Expression) = dateTime(e) millis
  def toDateTimeExpr = dateTime(unresolvedTimestampAttr)


  def unresolvedAnonymousCookieAttr = UnresolvedAttribute(anonymousCookieAttr)
  def partitionByAnonymousCookie : Seq[Expression] = Seq(unresolvedAnonymousCookieAttr)

  def sortByTimestamp : Seq[SortOrder] = Seq(
    //SortOrder(UnresolvedFunction(toDateTimeFuncName, Seq(timestampAttr)), Ascending))
    SortOrder(toDateTimeInMillis(unresolvedTimestampAttr), Ascending))

  // date expressions
  def parseYearExpr : NamedExpression = (toDateTimeExpr year) as (yearAttr)
  def parseMonthExpr : NamedExpression = (toDateTimeExpr monthOfYear) as (monthAttr)
  def parseDayExpr : NamedExpression = (toDateTimeExpr dayOfMonth) as (dayAttr)
  def parseDayNameExpr : NamedExpression = (toDateTimeExpr dayOfWeek) as (dayNameAttr)
  def parseHourOfDayExpr : NamedExpression = (toDateTimeExpr hourOfDay) as (hourAttr)

  //lazy val pagePath : Expression = UnresolvedExtractValue(UnresolvedExtractValue('context, "page"), "path")
  lazy val pagePath : Expression = UnresolvedAttribute(pagePathAttr)
  lazy val pageTitle : Expression = UnresolvedAttribute(pageNameAttr)
  lazy val pageReferrer : Expression = UnresolvedAttribute(pageReferrerAttr)
  lazy val eventPath : Expression = UnresolvedAttribute(eventNameAttr)

}

trait SessionizeExpressions {
  self : BaseExpressions =>

  def sessionMatchExpression : Predicate
  def sessionIdExpression : Expression
  def sessionIdAttr : String = "sd_session_id"

  def unresolvedSessionIdAttr = UnresolvedAttribute(sessionIdAttr)
  def partitionBySessionId : Seq[Expression] = Seq(unresolvedSessionIdAttr)

}

trait SessionMetricsExpressions {
  self : BaseExpressions with SessionizeExpressions =>
  /*
  def sessionRankByTimeAttrName : String
  def sessionRankbyTime = rank(partitionBySessionId, sortByTimestamp, sessionRankByTimeAttrName)
  */
  def bounceThreshold : Integer
  def sessionRowNumberByTimeAttr : String = "sd_session_rank"
  def sessionRowNumberByTimeExpr = rowNumber(partitionBySessionId, sortByTimestamp, sessionRowNumberByTimeAttr)

  def sessionStartTimeAttr : String = "sd_session_start_time"
  def sessionStartTimeExpr = firstValue(toDateTimeInMillis(unresolvedTimestampAttr),
    partitionBySessionId, sortByTimestamp, sessionStartTimeAttr)

  def sessionEndTimeAttr : String = "sd_session_end_time"
  def sessionEndTimeExpr = lastValue(toDateTimeInMillis(unresolvedTimestampAttr), partitionBySessionId, sortByTimestamp, sessionEndTimeAttr)

  def sessionLandingScreenAttr: String = "sd_session_landing_screen"
  def sessionLandingScreenExpr = firstValue(pagePath, partitionBySessionId, sortByTimestamp, sessionLandingScreenAttr)

  def sessionExitScreenAttr: String = "sd_session_exit_screen"
  def sessionExitScreenExpr = lastValue(pagePath, partitionBySessionId, sortByTimestamp, sessionExitScreenAttr)

  def sessionEventCountAttr: String = "sd_session_event_count"
  def sessionEventCountExpr = countStar(sessionIdAttr, partitionBySessionId, sortByTimestamp, sessionEventCountAttr)

  def sessionEventPathAttr : String = "sd_session_event_path"
  def sessionEventPathExpr = windowFnInvocation("collect_list", Seq(eventPath), partitionBySessionId, sortByTimestamp, sessionEventPathAttr)

  def sessionPagePathAttr : String = "sd_session_page_path"
  def sessionPagePathExpr = windowFnInvocation("collect_list", Seq(pagePath), partitionBySessionId, sortByTimestamp, sessionPagePathAttr)

  def sessionPageAttr : String = "sd_session_page_name"
  def sessionPagesExpr = windowFnInvocation("collect_list", Seq(pageTitle), partitionBySessionId, sortByTimestamp, sessionPageAttr)

  def sessionPageReferrerAttr : String = "sd_session_page_referrer"
  def sessionPageReferrerExpr =  firstValue(pageReferrer, partitionBySessionId, sortByTimestamp, sessionPageReferrerAttr)

  def sessionProductListAttr: String = "sd_session_product_list"
  def sessionProductsExpr = windowFnInvocation("collect_list", Seq(UnresolvedAttribute(productNameAttr)),
    partitionBySessionId, sortByTimestamp, sessionProductListAttr)

  def sessionProductQuantityAttr: String = "sd_session_product_quantity"
  def sessionProductQuantityExpr = windowFnInvocation("sum", Seq(UnresolvedAttribute(productQuantityAttr)),
    partitionBySessionId, sortByTimestamp, sessionProductQuantityAttr)

  def sessionRevAttr : String = "sd_session_revenue"
  def sessionRevenueExpr = windowFnInvocation("sum", Seq(UnresolvedAttribute(productRevenueAttr)),
    partitionBySessionId, sortByTimestamp, sessionRevAttr )

  def isRevSessionAttr : String = "sd_is_revenue_session"
  def isRevSessionExpr = windowFnInvocation("max", Seq(UnresolvedAttribute(isRevEventAttr)),
    partitionBySessionId, sortByTimestamp, isRevSessionAttr)

  val isCartSessionAttr : String = "sd_is_cart_session"
  def isCartSessionExpr = windowFnInvocation("max", Seq(UnresolvedAttribute(isCartEventAttr)),
    partitionBySessionId, sortByTimestamp, isCartSessionAttr)

  def isVideoSessionAttr : String = "sd_is_video_session"
  def isVideoSessionExpr = windowFnInvocation("max", Seq(UnresolvedAttribute(isVideoEventAttr)),
    partitionBySessionId, sortByTimestamp, isVideoSessionAttr)

  def sessionDurationAttr : String = "sd_session_duration"
  def sessionDurationExpr = Alias(
    UnresolvedFunction("round", Seq(Divide(Subtract(sessionEndTimeExpr, sessionStartTimeExpr), Literal(1000)), Literal(2))),
    sessionDurationAttr)()

  def isBounceSessionAttr : String = "sd_is_bounce_session"
  /*def isBounceSessionExpr = Alias(LessThan(Subtract(sessionEndTimeExpr, sessionStartTimeExpr), Literal(bounceThreshold)),
    isBounceSessionAttr)()*/
  def isBounceSessionExpr = Alias(CaseWhen(
    Seq(LessThan(sessionDurationExpr, Literal(bounceThreshold)), Literal(1), Literal(0))), isBounceSessionAttr)()

  def avgTimePerEventAttr : String = "sd_avg_time_per_event"
  def avgTimePerEventExpr = Alias(
    UnresolvedFunction("round", Seq(Divide(sessionDurationExpr, sessionEventCountExpr), Literal(2))),
    avgTimePerEventAttr)()

  def filterSessionRows = EqualTo(UnresolvedAttribute(sessionRowNumberByTimeAttr), Literal(1))
}


trait UserMetricsExpressions {
  self : BaseExpressions with SessionizeExpressions with SessionMetricsExpressions =>
  /*
    sum(sd_session_revenue) as sd_daily_revenue,
    sum(sd_session_product_quantity) as sd_daily_product_quantity,
    count(distinct sd_session_id) as sd_num_sessions,
    sum(sd_session_duration) as sd_daily_spent,
    avg(sd_session_duration) as sd_avg_session_time,
    sum(sd_session_num_events) as sd_daily_num_events,
    avg(sd_session_num_events) as sd_avg_num_events_per_session,
    max(sd_session_end_time) as sd_last_session_end_time,
    min(sd_session_start_time) as sd_earliest_session_start_time,
    sum(sd_is_revenue_session) as sd_num_revenue_session,
    sum(sd_is_cart_session) as sd_num_cart_session,
    sum(sd_is_video_session) as sd_num_video_session
   */
  def numberOfTiles : Integer
  // User Session Metrics
  val dailyRevenueAttr = "sd_daily_revenue"
  val dailyProductQuantityAttr = "sd_daily_product_quantity"
  val dailySessionCountAttr = "sd_session_count"
  val dailyTimeSpentAttr = "sd_time_spent"
  val avgSessionTimeAttr = "sd_avg_session_time"
  val dailyEventCountAttr = "sd_event_count"
  val avgNumEventsPerSessionAttr = "sd_avg_num_events_per_session"
  val lastSeenAtAttr = "sd_last_session_end_time"
  val fistSeenAtAttr = "sd_earliest_session_start_time"
  val dailyRevenueSessionCountAttr = "sd_num_revenue_session"
  val dailyVideoSessionCountAttr = "sd_num_video_session"
  val dailyCartSessionCountAttr = "sd_num_cart_session"
  //ranking columns
  val dailyUserRankByEventCount = "sd_rank_by_event_count"
  val dailyUserRankBySessionCount = "sd_rank_by_session_count"
  val dailyUserRankByTimeSpent = "sd_rank_by_time_spent"


  def dailyRevenueExpr : NamedExpression= Alias(Sum(UnresolvedAttribute(sessionRevAttr)), dailyRevenueAttr)()

  def dailyProductQuantityExpr : NamedExpression = Alias(Sum(UnresolvedAttribute(sessionProductQuantityAttr)), dailyProductQuantityAttr)()

  def dailySessionCountExpr : NamedExpression = Alias(CountDistinct(Seq(UnresolvedAttribute(sessionIdAttr))), dailySessionCountAttr)()

  def dailySpentExpr : NamedExpression  = Alias(Sum(UnresolvedAttribute(sessionDurationAttr)), dailyTimeSpentAttr)()

  def avgSessionTimeExpr : NamedExpression = Alias(UnresolvedFunction("round",
    Seq(Average(UnresolvedAttribute(sessionDurationAttr)), Literal(2))), avgSessionTimeAttr)()

  def dailyEventCountExpr : NamedExpression = Alias(Sum(UnresolvedAttribute(sessionEventCountAttr)), dailyEventCountAttr)()

  def avgNumEventsPerSessionExpr : NamedExpression = Alias(UnresolvedFunction("round",
    Seq(Average(UnresolvedAttribute(sessionEventCountAttr)), Literal(2))), avgNumEventsPerSessionAttr)()

  def lastSeenAtExpr : NamedExpression = Alias(Max(UnresolvedAttribute(sessionEndTimeAttr)), lastSeenAtAttr)()

  def firstSeenAtExpr : NamedExpression = Alias(Min(UnresolvedAttribute(sessionStartTimeAttr)), fistSeenAtAttr)()

  def dailyRevenueSessionCountExpr : NamedExpression = Alias(Sum(UnresolvedAttribute(isRevSessionAttr)),
    dailyRevenueSessionCountAttr)()

  def dailyCartSessionCountExpr : NamedExpression = Alias(Sum(UnresolvedAttribute(isCartSessionAttr)), dailyCartSessionCountAttr)()

  def dailyVideoSessionCountExpr : NamedExpression = Alias(Sum(UnresolvedAttribute(isVideoSessionAttr)), dailyVideoSessionCountAttr)()

  def sortByDailyEventCountAttr : Seq[SortOrder] = Seq(SortOrder(UnresolvedAttribute(dailyEventCountAttr), Ascending))
  def ntileDailyEventCountEpr = windowFnInvocation("ntile", Seq(Literal(numberOfTiles)), Seq(), sortByDailyEventCountAttr, dailyUserRankByEventCount, UnspecifiedFrame)

  def sortByDailySessionCountAttr : Seq[SortOrder] = Seq(SortOrder(UnresolvedAttribute(dailySessionCountAttr), Ascending))
  def ntileDailySessionCountEpr = windowFnInvocation("ntile", Seq(Literal(numberOfTiles)), Seq(), sortByDailySessionCountAttr, dailyUserRankBySessionCount, UnspecifiedFrame)

  def sortByDailyTimeSpentAttr : Seq[SortOrder] = Seq(SortOrder(UnresolvedAttribute(dailyTimeSpentAttr), Ascending))
  def ntileDailyTimeSpentEpr = windowFnInvocation("ntile", Seq(Literal(numberOfTiles)), Seq(), sortByDailyTimeSpentAttr, dailyUserRankByTimeSpent, UnspecifiedFrame)

}

trait ReveuneExpressions {
  self: BaseExpressions =>

  def revenueEquation = Multiply(UnresolvedAttribute(productQuantityAttr), UnresolvedAttribute(productPriceAttr))

  def revenueExpr = Alias(
    CaseWhen(
      Seq(
        EqualTo(UnresolvedAttribute(eventNameAttr), Literal(revenueEventName)),
        revenueEquation,
        Literal(0)
      )
      ), productRevenueAttr)()
}

trait GoalExpressions {
  self: BaseExpressions =>

  // goal product cube related
  def impactLookbackDays : Integer
  def impactEventAttrs : Seq[String]
  def impactEventAttrsAliases : Map[String, String]
  def impactEventTimeAttrName : String
  def impactEventTimeExpr = dateTime(UnresolvedAttribute(impactEventTimeAttrName))

  def goalEventTimeAttrName : String
  def goalEventTimeExpr = dateTime(UnresolvedAttribute(goalEventTimeAttrName))
  def goalEventAttrs : Seq[String]
  def goalEventAttrsAliases : Map[String, String]

  def goalJoinOnCols : Tuple2[String, String]
  def sessionIdJoinOnCols : Tuple2[String, String]

  def goalEventName : String
  def impactEventNames : Set[Any]
  def excludeFromImpactEvents : Set[Any]
  def excludeFromImpactPages : Set[Any]

  val goalEventTypeFilterExpr : Expression = EqualTo(UnresolvedAttribute(eventNameAttr), Literal(goalEventName))

  val productQuantityByGoalExpr : NamedExpression = Alias(Sum(UnresolvedAttribute(productQuantityAttr)), productQuantityAttr)()
  val productRevenueByGoalExpr : NamedExpression = Alias(Sum(UnresolvedAttribute(productRevenueAttr)), productRevenueAttr)()
  def goalEventAggrExpr : Seq[NamedExpression] = Seq(productQuantityByGoalExpr, productRevenueByGoalExpr)
  def goalMetrics : Seq[NamedExpression]
}





trait ETLExpressions extends BaseExpressions with SessionizeExpressions
  with SessionMetricsExpressions with UserMetricsExpressions with ReveuneExpressions with GoalExpressions{


  // user agent parsing related
  def userAgentAttr : String
  def parseUserAgentExpr : NamedExpression =
    UnresolvedFunction("parseUserAgent", Seq(UnresolvedAttribute(userAgentAttr))).as(eUserAgentAttr)

  // ip address parsing
  def parseIpExpr : NamedExpression =
    UnresolvedFunction("parseIp", Seq(UnresolvedAttribute(ipAttr))).as(eIpAttr)


  //def parseSparkDateExpr : NamedExpression = dateTime(timestampAttr) as ("sd_utc_time")
  //def parseSparkDatePSTExpr : NamedExpression = ((dateTime(timestampAttr) - 8.hour) withZone("US/Pacific")) as ("sd_pst_time")
}
