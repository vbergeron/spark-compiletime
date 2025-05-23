ThisBuild / scalaVersion := "3.7.0"

// Compile / compile / javaHome :=

// Dependencies
lazy val spark = Seq(
  ("org.apache.spark" %% "spark-core" % "3.5.5").cross(CrossVersion.for3Use2_13),
  ("org.apache.spark" %% "spark-sql"  % "3.5.5").cross(CrossVersion.for3Use2_13)
)

lazy val munit = Seq(
  ("org.scalameta" %% "munit" % "1.1.0")
)

lazy val `spark-compiletime` = (project in file("."))
  .settings(libraryDependencies ++= Seq.concat(spark, munit.map(_ % Test)))

lazy val exemples = (project in file("exemples"))
  .dependsOn(`spark-compiletime`)

ThisBuild / organization := "com.github.sbt"
ThisBuild / homepage     := Some(url("https://github.com/sbt/sbt-ci-release"))
ThisBuild / licenses     := List("BSD 3-Clause" -> url("https://opensource.org/license/bsd-3-clause"))
ThisBuild / developers   := List(
  Developer(
    "vbergeron",
    "Valentin Bergeron",
    "bergeron.valentin@gmail.com",
    url("https://github.com/vbergeron")
  )
)

// Workaround sbt-sonatype
ThisBuild / sonatypeCredentialHost := "s01.oss.sonatype.org"
sonatypeRepository                 := "https://s01.oss.sonatype.org/service/local"
