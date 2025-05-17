ThisBuild / scalaVersion := "3.7.0"

// Dependencies
lazy val spark = Seq(
  ("org.apache.spark" %% "spark-core" % "3.5.5").cross(CrossVersion.for3Use2_13),
  ("org.apache.spark" %% "spark-sql"  % "3.5.5").cross(CrossVersion.for3Use2_13)
)

lazy val `spark-scala3-compat` = (project in file("."))
  .settings(libraryDependencies ++= spark)
