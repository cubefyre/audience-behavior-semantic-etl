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

import org.apache.spark.Logging
import org.scalatest.{BeforeAndAfterAll, GivenWhenThen}
import org.sparkline.base.{QueryTest, TestSparkline}
import org.sparkline.customers.acme._

class AcmeETLProcessTest extends QueryTest with BeforeAndAfterAll with GivenWhenThen with Logging {


  override def beforeAll() = {
    functions.register(TestSparkline)
  }

  /*
   * to run copy a day's worth of data from s3
   * aws s3 cp --recursive s3://acme.logs.segment.production/segment-logs/VhBYQcAjH5/1432684800000/ /tmp/1432684800000
   */
  test("dailyProcess") {

    val tP = new DailyCubesETL("/tmp/acme", "/tmp/acme-etl")(TestSparkline)

    val etlProcess = new ETLProcess(tP)

    etlProcess.run
  }
}
