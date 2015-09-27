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

 package org.sparkline.etl.geoip


import java.util.zip.GZIPInputStream
import com.maxmind.geoip2.DatabaseReader
import com.twitter.util.{LruMap, SynchronizedLruMap}
import java.io.InputStream
import java.net.InetAddress


/**
 * Copied from https://github.com/Sanoma-CDA
 * Cleanup/simplify.
 *
 * @param dbInputStream The DB file unzipped
 * @param lruCache The Size of the LRU cache
 * @param synchronized Use synchronized (true) for multithreaded environments
 * @param postFilterIpLocationObject Optional function that analyzes the IpLocation and possible changes the field values
 */
class MaxMindIpGeo(dbInputStream: InputStream, lruCache: Int = 10000, synchronized: Boolean = false,
                   postFilterIpLocationObject: Option[(IpLocation) => Option[IpLocation]] = None) {

  protected val maxmind = new DatabaseReader.Builder(dbInputStream).build

  val validNum = """(25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9][0-9]|[0-9])"""
  val dot = """\."""
  val validIP = (validNum + dot + validNum + dot + validNum + dot + validNum).r

  def getInetAddress(address: String) = {
    try {
      address match {
        case validIP(_, _, _, _) => Some(InetAddress.getByAddress(address.split('.').map(_.toInt.toByte)))
        case _                   => Some(InetAddress.getByName(address))
      }
    } catch {
      case _ : Throwable => None
    }
  }

  private def getLocationFromDB(address: String) = try {
    getInetAddress(address).map(maxmind.city(_))
  } catch {
    case _ : Throwable => None // we don't really care about which exception we got
  }

  private val lru = if (lruCache > 0) {
    if (synchronized) {
      new SynchronizedLruMap[String, Option[IpLocation]](lruCache)
    }
    else
      {
        new LruMap[String, Option[IpLocation]](lruCache)
      }
  } else null

  private def getLocationFiltered(address: String): Option[IpLocation] =
    getLocationUnfiltered(address).flatMap(postFilterIpLocationObject.get)
  private def getLocationUnfiltered(address: String): Option[IpLocation] = getLocationFromDB(address).map(IpLocation(_))

  val getLocationWithoutLruCache = postFilterIpLocationObject match {
    case Some(filter) => getLocationFiltered _
    case None => getLocationUnfiltered _
  }

  def getLocationWithLruCache(address: String) = {
    lru.get(address) match {
      case Some(loc) => loc
      case None => {
        val loc = getLocationWithoutLruCache(address)
        lru.put(address, loc)
        loc
      }
    }
  }

  val getLocation: String => Option[IpLocation] =
    if (lruCache > 0) getLocationWithLruCache else getLocationWithoutLruCache

}

object MaxMindIpGeo {

  val GeoLite2CityDB = "/GeoLite2-City.mmdb.gz"

  def apply(lruCache: Int = 10000, synchronized: Boolean = false,
            postFilterIpLocationObject: Option[(IpLocation) => Option[IpLocation]] = None) = {

    new MaxMindIpGeo(new GZIPInputStream(getClass.getResource(GeoLite2CityDB).openStream()),
      lruCache, synchronized, postFilterIpLocationObject)
  }

}