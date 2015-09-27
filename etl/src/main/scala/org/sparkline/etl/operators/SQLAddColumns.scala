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

 package org.sparkline.etl.operators

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.logical.Project


/**
 * Created by Jitender on 7/14/15.
 */
object SQLAddColumns {

  def dataFrame(input : DataFrame,
                sqlQuery : String, tempTableName : String) : DataFrame = {

    input.registerTempTable(tempTableName)
    input.sqlContext.sql(sqlQuery)
  }

}
