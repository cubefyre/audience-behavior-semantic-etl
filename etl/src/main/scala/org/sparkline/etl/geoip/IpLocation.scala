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

import com.maxmind.geoip2.model.CityResponse

case class IpLocation(
                       countryCode: Option[String],
                       countryName: Option[String],
                       region: Option[String], // should look more into this...
                       city: Option[String],
                       geoPoint: Option[Point],
                       postalCode: Option[String],
                       continent: Option[String])


object IpLocation {

  // Doesn't make sense to only have Latitude or Longitude
  def combineLatLong(lat: Option[Double], lon: Option[Double]) = (lat, lon) match {
    case (Some(lat), Some(lon)) => Some(Point(lat,lon))
    case _ => None
  }

  def jDoubleOptionify(jd: java.lang.Double): Option[Double] = if (jd == null) None else Some(jd)

  def apply(omni: CityResponse): IpLocation = new IpLocation(
    if (omni.getCountry != null) Option(omni.getCountry.getIsoCode) else None,
    if (omni.getCountry != null) Option(omni.getCountry.getName) else None,
    if (omni.getMostSpecificSubdivision != null) Option(omni.getMostSpecificSubdivision.getName) else None,
    if (omni.getCity != null) Option(omni.getCity.getName) else None,
    if (omni.getLocation != null) combineLatLong(jDoubleOptionify(omni.getLocation.getLatitude),
      jDoubleOptionify(omni.getLocation.getLongitude)) else None,
    if (omni.getPostal != null) Option(omni.getPostal.getCode) else None,
    if (omni.getContinent != null) Option(omni.getContinent.getName) else None
  )
}