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
import ua_parser.{Parser, Client}

case class OS(family : String, major : String, minor : String, patch : String, patchMinor : String)
case class Device(family : String)
case class UserAgent(family : String, major : String, minor : String, patch : String, os : OS, device : Device)

object UserAgentFunctions {

  val uaParser = new Parser()

  def parseUserAgent(uaStr : String) : UserAgent = {
    try {
      val c: Client = uaParser.parse(uaStr)
      UserAgent(c.userAgent.family,
      c.userAgent.major,
      c.userAgent.minor,
      c.userAgent.patch,
      OS(c.os.family, c.os.major, c.os.minor, c.os.patch, c.os.patchMinor),
      Device(c.device.family))
    } catch {
      case e : Throwable => null
    }
  }

  def registerFunctions(sqlContext : SQLContext) : Unit = {
    sqlContext.udf.register("parseUserAgent", parseUserAgent _)
  }
}
