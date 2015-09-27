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
 import sbt._
import sbt.Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

object BuildSettings {

  lazy val sparklineUdafsProject = RootProject(uri("git://github.com/SparklineData/SparklineFunctions.git"))

  val buildSettings = Defaults.defaultSettings ++ Seq(
    organization := "org.sparklinedata",
    version := "1.0.1-SNAPSHOT",
    scalaVersion := "2.10.4",
    crossScalaVersions := Seq("2.10.2", "2.10.3", "2.10.4", "2.11.0", "2.11.1", "2.11.2", "2.11.3", "2.11.4", "2.11.5"),
    resolvers += Resolver.sonatypeRepo("snapshots"),
    resolvers += Resolver.sonatypeRepo("releases"),
    resolvers += "Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository",
    resolvers += "JitPack.IO" at "https://jitpack.io",
    scalacOptions ++= Seq(),
    autoAPIMappings := true
  ) ++ net.virtualvoid.sbt.graph.Plugin.graphSettings

  val sparkVersion = "1.4.0"
  val hiveVersion = "1.1.0"
  val hadoopVersion = "2.4.0"
  val slf4jVersion = "1.7.10"
  val log4jVersion = "1.2.17"
  val protobufVersion = "2.4.1"
  val yarnVersion = hadoopVersion
  val hbaseVersion = "0.94.6"
  val akkaVersion = "2.3.10"
  val sprayVersion = "1.3.3"
  val jettyVersion = "8.1.14.v20131031"
  val jlineVersion = "2.12.1"
  val jlineGroupid = "jline"
  val json4sVersion = "3.2.11"
  val nscalaVersion = "1.6.0"
  val spklUdfsVersion = "0.0.1"
  val scalatestVersion = "2.2.4"
  val guavaVersion = "14.0.1"
  val eclipseJettyVersion = "8.1.14.v20131031"
  val sparkdateTimeVersion = "0.0.1"

  val explicitDependencies = Seq(
    "com.google.guava" % "guava" % guavaVersion % "provided",
    "org.eclipse.jetty" % "jetty-server" % eclipseJettyVersion % "provided",
    "org.eclipse.jetty" % "jetty-plus" % eclipseJettyVersion % "provided",
    "org.eclipse.jetty" % "jetty-util" % eclipseJettyVersion % "provided",
    "org.eclipse.jetty" % "jetty-http" % eclipseJettyVersion % "provided",
    "org.eclipse.jetty" % "jetty-servlet" % eclipseJettyVersion % "provided"
  )

}

object ETLBuild extends Build {

  import BuildSettings._

  lazy val root: Project = Project(
    "root",
    file("."),
    settings = buildSettings ++ Seq(
      run <<= run in Compile in etl)
  ) aggregate (etl)

  lazy val etl: Project = Project(
    "etl",
    file("etl"),
    settings = buildSettings ++ assemblySettings ++ Seq(
      libraryDependencies := libraryDependencies.value ++ explicitDependencies ++ Seq(
        "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
        "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
        "org.apache.spark" %% "spark-catalyst" % sparkVersion % "provided",
        "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",

        "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided" exclude("asm", "asm")
        exclude("org.ow2.asm", "asm") exclude("org.jboss.netty", "netty") exclude("commons-logging", "commons-logging")
          exclude("com.google.guava", "guava") exclude("org.mortbay.jetty", "servlet-api-2.5")
          exclude("javax.servlet", "servlet-api") exclude("junit", "junit"),

        "org.json4s" %% "json4s-native" % json4sVersion,
        "org.json4s" %% "json4s-ext" % json4sVersion,
        "com.github.nscala-time" %% "nscala-time" % nscalaVersion,

        "com.github.SparklineData" % "SparklineFunctions" % spklUdfsVersion,
        "com.github.SparklineData" % "spark-datetime" % sparkdateTimeVersion,

        "net.java.dev.jets3t" % "jets3t" % "0.9.0" exclude("commons-logging", "commons-logging"),
        "org.apache.httpcomponents" % "httpcore" % "4.3",
        "commons-io" % "commons-io" % "2.4",
        "com.github.SparklineData" % "uap-java" % "v1.1.0",
        "com.maxmind.geoip2" % "geoip2" % "2.1.0",
        "com.twitter" %% "util-collection" % "6.24.0",

        "org.slf4j" % "slf4j-api" % slf4jVersion,
        //"org.slf4j" % "slf4j-simple" % slf4jVersion,
        "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
        "org.slf4j" % "jul-to-slf4j" % slf4jVersion,
        "org.slf4j" % "jcl-over-slf4j" % slf4jVersion,
        "log4j" % "log4j" % log4jVersion,

        "org.scalatest" %% "scalatest" % scalatestVersion % "test",
        "org.scalacheck" %% "scalacheck" % "1.12.2" % "test"
      ),
      assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
      test in assembly := {},
      mergeStrategy in assembly := {
        case PathList("org", "datanucleus", xs @ _*)             => MergeStrategy.discard
        case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
        case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
        case "log4j.properties"                                  => MergeStrategy.discard
        case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
        case "reference.conf"                                    => MergeStrategy.concat
        case _                                                   => MergeStrategy.first
      }
    )
  ) //dependsOn(sparklineUdafsProject)

}
