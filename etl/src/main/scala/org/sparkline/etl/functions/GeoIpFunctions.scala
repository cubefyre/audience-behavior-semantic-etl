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
import org.sparkline.etl.geoip.{IpLocation, MaxMindIpGeo}

case class parseIpFunction(lruCache: Int = 10000, synchronized: Boolean = false) extends Function1[String,IpLocation] {
  @transient
  private var _geoIp : MaxMindIpGeo = null

  private def geoIp = {
    if (_geoIp == null) _geoIp = MaxMindIpGeo(1000, synchronized = true)
    _geoIp
  }

  def apply(ipStr : String) : IpLocation = {
    geoIp.getLocation(ipStr).getOrElse(null)
  }
}

object GeoIpFunctions {

  def registerFunctions(sqlContext : SQLContext) : Unit = {
    sqlContext.udf.register("parseIp",  parseIpFunction(1000, true))
  }

}
