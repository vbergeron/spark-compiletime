ThisBuild / scalaVersion  := "3.3.6"
ThisBuild / versionScheme := Some("early-semver")

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

ThisBuild / organization := "io.github.vbergeron"
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

ThisBuild / publishTo := {
  val centralSnapshots = "https://central.sonatype.com/repository/maven-snapshots/"
  if (isSnapshot.value) Some("central-snapshots" at centralSnapshots)
  else localStaging.value
}
