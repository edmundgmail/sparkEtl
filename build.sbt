import sbt.Keys._

name := "sparkEtl"

version := "0.1"

scalaVersion := "2.12.8"

resolvers ++= Seq(
  Resolver.mavenLocal,
  "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
  "releases" at "http://oss.sonatype.org/content/repositories/releases",
  "Maven repo1" at "https://repo1.maven.org/maven2")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.6" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.6" % "provided",
  "org.apache.spark" %% "spark-hive" % "2.4.6" % "provided",
  "org.apache.spark" %% "spark-catalyst" % "2.4.6" % "provided",
  "org.typelevel"        %% "simulacrum"      % "1.0.0",
  "org.yaml" % "snakeyaml" % "1.27",
  "org.typelevel" %% "cats-core" % "2.1.1" withSources() withJavadoc(),
  "org.typelevel" %% "cats-effect" % "2.1.4" withSources() withJavadoc(),
  "org.typelevel" %% "cats-effect-laws" % "2.1.1" % "test",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test"
)

scalacOptions ++= Seq(
  "-feature",
  "-deprecation",
  "-unchecked",
  "-language:_",
  "-Xfatal-warnings",
  "-Ypartial-unification")