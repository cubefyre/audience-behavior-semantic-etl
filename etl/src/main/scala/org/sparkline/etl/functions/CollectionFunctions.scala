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

import org.apache.hadoop.hive.ql.exec.FunctionRegistry
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver
import org.apache.spark.sql.SQLContext

object CollectionFunctions {

  def registerFunctions(sqlContext : SQLContext) : Unit = {
    val cls = Class.forName("org.sparkline.hive.udafs.GenericUDAFSumMaps").asInstanceOf[Class[GenericUDAFResolver]]

    FunctionRegistry.registerGenericUDAF(false,
      "sum_maps",
      cls.newInstance())
  }
}
