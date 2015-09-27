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

object Point {
  import Numeric.Implicits._
  import language.implicitConversions
  implicit def tuple22Point[A](p: (A, A))(implicit n:Numeric[A]) = new Point(p._1.toDouble, p._2.toDouble)
  implicit def point2Tuple2(p: Point) = (p.latitude, p.longitude)
}

/**
 * A geographical point
 * http://en.wikipedia.org/wiki/ISO_6709
 * x = latitude comes before y = longitude
 * height / depth optional
 * should have coordinate reference system, but we'll try to keep this simple (although, see circle)
 * @param latitude
 * @param longitude
 */
case class Point(latitude: Double, longitude: Double)

object funcs {
  import math.{sin, cos, atan2, pow, toRadians, sqrt}

  val EarthRadius = 6371009.0 // meters, http://en.wikipedia.org/wiki/Earth_radius#Mean_radius

  /**
   * Distance using the simple pythagoran formula
   * Really useable only for short distances (what is short?) - but fast
   * // http://en.wikipedia.org/wiki/Geographical_distance#Spherical_Earth_projected_to_a_plane
   * @param p1
   * @param p2
   * @return Distance in meters
   */
  def distanceSphericalEarth(p1: Point, p2: Point) = {
    // phi = lat, lamda = long
    val dLatRad = toRadians(p2.latitude - p1.latitude)
    val dLngRad = toRadians(p2.longitude - p1.longitude)
    val meanLat = toRadians(p2.latitude + p1.latitude) / 2.0
    EarthRadius * sqrt(pow(dLatRad, 2) + pow(cos(meanLat) * dLngRad, 2))
  }

  /**
   * Distance using Haversine formula
   * http://en.wikipedia.org/wiki/Haversine_formula
   * @param p1
   * @param p2
   * @return Distance in meters
   */
  def distanceHaversine(p1: Point, p2: Point) = {
    @inline def haversin(phi: Double) = pow(sin(phi / 2.0), 2.0)
    val dLatRad = toRadians(p2.latitude - p1.latitude)
    val dLngRad = toRadians(p2.longitude - p1.longitude)
    val p1LatRad = toRadians(p1.latitude)
    val p2LatRad = toRadians(p2.latitude)
    val a = haversin(dLatRad) + cos(p1LatRad) * cos(p2LatRad) * haversin(dLngRad)
    val c = 2 * atan2(sqrt(a), sqrt(1-a))
    EarthRadius * c
  }
}