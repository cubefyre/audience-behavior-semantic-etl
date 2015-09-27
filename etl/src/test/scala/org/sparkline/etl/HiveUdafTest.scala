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

import org.apache.commons.io.FileUtils
import org.apache.hadoop.hive.ql.exec.FunctionRegistry
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver
import org.apache.spark.Logging
import org.joda.time.DateTime
import org.scalatest.{GivenWhenThen, BeforeAndAfterAll}
import org.sparkline.base.{TestHive, QueryTest}

import org.sparkline.utils.ResourceUtils

class HiveUdafTest extends QueryTest with BeforeAndAfterAll with GivenWhenThen with Logging {

  import TestHive.{read, sql, udf}

  var eventsDir : File = _

  override def beforeAll() = {

    udf.register("spklDateTme", (s : String) => new DateTime(s).getMillis)

    val cls = Class.forName("org.sparkline.hive.udafs.GenericUDAFSumMaps").asInstanceOf[Class[GenericUDAFResolver]]

    FunctionRegistry.registerGenericUDAF(false,
      "sum_maps",
      cls.newInstance())

    eventsDir = ResourceUtils.createTempDir()
    FileUtils.copyInputStreamToFile(getClass.getResource("/events.json.gz").openStream(),
      new File(eventsDir, "events.json.gz"))
  }

  ignore("hiveUdaf") {
    val jsonDF = read.json(eventsDir.getCanonicalPath)

    jsonDF.registerTempTable("eventLog")

    var df1 = sql("select session_id, action_profile['postPhoto'] as photosPosted, action_profile from (" +
      "select session.id as session_id, sum_maps(map(event.name, 1)) as action_profile from eventLog group by session.id) s ")


    df1.take(10).foreach { r =>
      println(r)
    }

  }
}
