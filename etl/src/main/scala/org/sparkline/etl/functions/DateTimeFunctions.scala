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
 package org.sparkline.etl.functions

import org.apache.spark.sql.SQLContext
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.format._

object DateTimeFunctions {

  def spklDateTme(s : String) = new DateTime(s).getMillis

  def spklFormatDate(s : String) : String = {
  //def spklFormatDate(dt : DateTime) : String = {
    val fmt = DateTimeFormat.forPattern("YYYY-MM-dd")
    val dt = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC).parseDateTime((s))
    fmt.print(dt)
  }

  def registerFunctions(sqlContext : SQLContext) : Unit = {
    sqlContext.udf.register("spklDateTme", spklDateTme _)
    sqlContext.udf.register("spklFormatDate", spklFormatDate _)
  }
}
