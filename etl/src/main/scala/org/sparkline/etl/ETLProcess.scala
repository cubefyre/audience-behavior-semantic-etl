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
 * Co-authored by Harish.
 * Co-authored by Jitender on 7/14/15.
 */

 package org.sparkline.etl

import java.io.File

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.{SaveMode, DataFrame}
import org.joda.time.DateTime
import org.sparkline.utils.PartitionUtils

import scala.collection.mutable.ArrayBuffer

case class ETLStep(name : String, description : String,
                    output : DataFrame,
                    inputSteps : Seq[ETLStep],
                    persistResult : Boolean = false,
                    partitionByColumns : Seq[String] = Seq())
  extends Iterable[ETLStep] {

  self =>

  def dataFrame = {
    run
    cachedDF.get
  }

  private var cachedDF : Option[DataFrame] = None

  def queryExecution = output.queryExecution

  lazy val plan: LogicalPlan = queryExecution.analyzed
  lazy val optimizedPlan = queryExecution.optimizedPlan
  lazy val physicalPlan = queryExecution.sparkPlan

  def generateTreeString(depth: Int, builder: StringBuilder): String = {
    builder.append(" " * depth)
    builder.append(name)
    builder.append("\n")
    inputSteps.foreach(_.generateTreeString(depth + 1, builder))
    builder.toString()
  }

  override def toString: String = generateTreeString(0, new StringBuilder)

  def describe: String = {
    s"""$name
        |schema: ${plan.schemaString}
        |plan:
        |${plan.treeString}
     """.stripMargin
  }

  override def foreach[U](f: ETLStep => U): Unit = {
    f(this)
    inputSteps.foreach(_.foreach(f))
  }

  def iterator: Iterator[ETLStep] = new Iterator[ETLStep] {

    val queue: ArrayBuffer[ETLStep] = ArrayBuffer(self)

    override def hasNext: Boolean = !queue.isEmpty

    override def next(): ETLStep = {
      val n = queue.remove(0)
      queue.append(self.inputSteps: _*)
      n
    }
  }

  def reverseIterator : Iterator[ETLStep] = {
    inputSteps.foldRight(Iterator[ETLStep](this))((a,b) => a.reverseIterator ++ b)
  }

  /**
   * recreate a DataFrame, so that cached RDDs are applied to the physical Plan.
   */
  def run : Unit = {
    if ( !cachedDF.isDefined ) {
      inputSteps.foreach(_.run)
      cachedDF = Some(new DataFrame(output.sqlContext, output.queryExecution.logical))
      cachedDF.get.cache()
    }
  }

  def persist(outputPath : String) : Unit = {
    //run
    //inputSteps.foreach(_.persist(outputPath))
    // Seq(1, 2, 3): _*
    if(persistResult) {

      if(partitionByColumns.isEmpty) {
        cachedDF.get.coalesce(1).write.mode(SaveMode.Append).parquet(outputPath)
      } else {
        cachedDF.get.coalesce(1).write.partitionBy(partitionByColumns: _*).mode(SaveMode.Append).parquet(outputPath)
      }
      cachedDF.get.registerTempTable(name)
      println(
        s"""
           |Persisted dataframe : $name and created temp table.
         """.stripMargin)
    }
  }

  def unpersist : Unit = {
    if ( cachedDF.isDefined) {
      cachedDF.get.unpersist()
    }
  }

}

trait ETLIOInfo {

  def rawDataBasePath : String
  def analyticsBasePath : String
  //def dayInMills : Long
  
  //lazy val baseETLDir = new File(baseETLDirName)
  //lazy val inputPath = new Path(rawDataPath)
  //lazy val rawDataPath = rawDataBasePath + "/" + dayInMills

  /*lazy val (year : Int, month : Int, day : Int) = {
    //val epoch = inputPath.getName.toLong
    val dt = new DateTime(dayInMills)
    (dt.getYear, dt.getMonthOfYear, dt.getDayOfMonth)
  }*/
}


trait ETLInfo extends ETLIOInfo {

  def etlExpressions: ETLExpressions

  def rawEventsStep: ETLStep

  def enrichedRawEventsStep : ETLStep

  def sessionizedEventsStep: ETLStep

  def sessionDailyMetricsStep : ETLStep

  def userDailyMetricsStep : ETLStep

  def steps : Seq[ETLStep] = Seq()


}


class ETLProcess(val etlInfo : ETLInfo) {

  /*
   * trigger validation of the input path.
   * assert(etlInfo.year != null)
   */

  val etlStepMap : Map[String, ETLStep] = etlInfo.steps.map(s => (s.name,s)).toMap

  val childStepMap : Map[String, Seq[ETLStep]] = {
    val m = scala.collection.mutable.Map[String, Seq[ETLStep]]()
    etlInfo.steps.foreach { s=>
      s.inputSteps.foreach { i =>
        val l : Seq[ETLStep] = m.getOrElse(i.name, Seq[ETLStep]())

        m(i.name) = (l :+ s)
      }
    }
    m.toMap
  }

  lazy val finalSteps = etlInfo.steps.filter(s => !childStepMap.contains(s.name))

  lazy val rootSteps = etlInfo.steps.filter(s => s.inputSteps.size == 0)

  def showETLPlan: String = {
    val b = new StringBuilder
    rootSteps.foreach { s =>
      s.generateTreeString(0, b).toString()
    }
    b.toString()
  }

  def describeStep(name: String) = etlStepMap(name).describe

  def run : Unit = {
    finalSteps.foreach { rs =>
      rs.reverseIterator.foreach { s =>
        runETLStep(s)
      }
    }
  }

  private def runETLStep(step : ETLStep) : Unit = {
    step.run
    if (step.persistResult) {
      step.persist(Seq(etlInfo.analyticsBasePath, step.name).mkString("/"))
    }
  }

  def unpersist : Unit = {
    finalSteps.foreach { rs =>
      rs.reverseIterator.foreach { step =>
        if (step.persistResult) {
          step.unpersist
        }
      }
    }
  }

}
