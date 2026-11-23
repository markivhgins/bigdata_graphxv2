ThisBuild / version := "1.0.0"
ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "edu.college.socialanalyzer"

lazy val root = (project in file("."))
  .settings(
    name := "social-network-graphx",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.3" % "provided",
      "org.apache.spark" %% "spark-graphx" % "3.5.3" % "provided"
    ),
    Compile / mainClass := Some("com.socialanalyzer.SocialNetworkAnalyzer"),
    scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked")
  )
