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
import org.apache.spark.sql.{DataFrame, SaveMode}

import org.joda.time.DateTime
import org.scalatest.{GivenWhenThen, BeforeAndAfterAll}
import org.sparkline.base.TestHive._
import org.sparkline.base.{TestHive, QueryTest}
import org.sparkline.etl.operators.DropColumns
import org.sparkline.utils.PartitionUtils


class ParquetFileTest extends QueryTest with BeforeAndAfterAll with GivenWhenThen with Logging {

  import TestHive.{read, sql, udf}
  import TestHive.implicits.{localSeqToDataFrameHolder, rddToDataFrameHolder}

  val outDirName = "/tmp/acme.parquet"
  val inDirName = "/tmp/1432684800000"

  override def beforeAll() = {

    udf.register("spklDateTme", (s : String) => new DateTime(s).getMillis)

    val cls = Class.forName("org.sparkline.hive.udafs.GenericUDAFSumMaps").asInstanceOf[Class[GenericUDAFResolver]]

    FunctionRegistry.registerGenericUDAF(false,
      "sum_maps",
      cls.newInstance())
  }

  test("map") {
    val data = (1 to 4).map(i => Tuple1(Map(i -> s"val_$i")))
    sparkContext.parallelize(data).toDF().write.mode(SaveMode.Overwrite).parquet("/tmp/a")
  }


  ignore("writeParquet") {
    var jsonDF = read.json(inDirName)
    jsonDF = DropColumns.dataFrame(jsonDF, Seq("integrations"))
    jsonDF.write.mode(SaveMode.Overwrite).parquet(outDirName)
  }


  /*
   * TODO when reading back, why is partitioner not set?
   */
  ignore("writeReadPartitions") {

    var jsonDF = read.json(inDirName)
    jsonDF = DropColumns.dataFrame(jsonDF, Seq("integrations"))

    var outDir = PartitionUtils.getPartitionDir(new java.io.File(outDirName),
      ("year", 2015),
      ("month", 5),
      ("day", 26)
    )
    jsonDF.write.mode(SaveMode.Overwrite).parquet(outDir.getCanonicalPath)

    outDir = PartitionUtils.getPartitionDir(new java.io.File(outDirName),
      ("year", 2015),
      ("month", 5),
      ("day", 27)
    )
    jsonDF.write.mode(SaveMode.Overwrite).parquet(outDir.getCanonicalPath)

    jsonDF = read.parquet(outDirName)

    println(jsonDF.rdd.partitions)

    println(jsonDF.schema)

    println(jsonDF.count())
  }

  ignore("partitionedParquetSampleEg") {

    // sqlContext from the previous example is used in this example.
    // This is used to implicitly convert an RDD to a DataFrame.
    import TestHive.implicits._

    // Create a simple DataFrame, stored into a partition directory
    val df1 : DataFrame = sparkContext.makeRDD(1 to 5).map(i => (i, i * 2)).toDF("single", "double")
    df1.write.mode(SaveMode.Overwrite).parquet("data/test_table/key=1")

    // Create another DataFrame in a new partition directory,
    // adding a new column and dropping an existing column
    val df2 = sparkContext.makeRDD(6 to 10).map(i => (i, i * 3)).toDF("single", "triple")
    df2.write.mode(SaveMode.Overwrite).parquet("data/test_table/key=2")

    // Read the partitioned table
    val df3 = TestHive.read.parquet("data/test_table")
    df3.printSchema()



    // The final schema consists of all 3 columns in the Parquet files together
    // with the partiioning column appeared in the partition directory paths.
    // root
    // |-- single: int (nullable = true)
    // |-- double: int (nullable = true)
    // |-- triple: int (nullable = true)
    // |-- key : int (nullable = true)
  }

}
