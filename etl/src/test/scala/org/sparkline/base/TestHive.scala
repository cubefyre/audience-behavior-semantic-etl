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

 package org.sparkline.base

import org.apache.spark.sql.hive.test.TestHiveContext
import org.apache.spark.{SparkConf, SparkContext}

object TestSparkContext {

  def newContext = new SparkContext("local[2]", "TestSQLContext",
    new SparkConf().set("spark.sql.test", "").
      set("spark.jars",
        "/Users/hbutani/.m2/repository.sav/org/sparkline/hive/udfs/0.0.1-SNAPSHOT/udfs-0.0.1-SNAPSHOT.jar")/*.
        - prefix hadoop options with spark.hadoop
        - for a while thought SPARK-8093 issue was related to parallelism
      set("spark.hadoop.parquet.metadata.read.parallelism", "1")*/)
}

object TestHive extends TestHiveContext(TestSparkContext.newContext)


object TestSparkline extends TestHiveContext(TestSparkContext.newContext) {

  def runHive(cmd: String, maxRows: Int = 1000): Seq[String] =
    super.runSqlHive(cmd)
}