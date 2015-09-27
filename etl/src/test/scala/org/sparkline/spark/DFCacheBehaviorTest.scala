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

 package org.sparkline.spark

import org.apache.hadoop.hive.ql.exec.FunctionRegistry
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver
import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame
import org.joda.time.DateTime
import org.scalatest.{GivenWhenThen, BeforeAndAfterAll}
import org.sparkline.base.{TestHive, QueryTest}
import org.sparkline.customers.acme.AcmeETLExpressions
import org.sparkline.etl.operators.{Sessionize, Lag}

class DFCacheBehaviorTest extends QueryTest with BeforeAndAfterAll with GivenWhenThen with Logging {

  import TestHive.{read, sql, udf}

  override def beforeAll() = {

    udf.register("spklDateTme", (s : String) => new DateTime(s).getMillis)

    val cls = Class.forName("org.sparkline.hive.udafs.GenericUDAFSumMaps").asInstanceOf[Class[GenericUDAFResolver]]

    FunctionRegistry.registerGenericUDAF(false,
      "sum_maps",
      cls.newInstance())
  }


  test("acmeSessionizePlan") {

    val jsonDF = read.json("/tmp/1436140800000/")
    val eL = jsonDF.repartition(1).cache

    val lagDF = Lag.lagOnTimeStamp(eL,
      AcmeETLExpressions,
      1)

    println(lagDF.rdd.partitioner)


    val sessionizeDF = Sessionize.dataFrame(lagDF,
      AcmeETLExpressions)

    println(sessionizeDF.rdd.partitioner)

    println("Before cache")
    println(sessionizeDF.queryExecution.executedPlan)

    lagDF.cache()

    println("After cache")
    println(new DataFrame(sessionizeDF.sqlContext, sessionizeDF.queryExecution.logical).queryExecution.executedPlan)


  }
}
