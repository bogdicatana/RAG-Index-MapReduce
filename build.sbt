import sbt.Keys.libraryDependencies

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.3.6"

lazy val root = (project in file("."))
    .settings(
        name := "cs441hw1",
        libraryDependencies ++= Seq(
            // Retrieval index (pure JVM, cross-platform)
            "org.apache.lucene" % "lucene-core" % "10.3.0",
            "org.apache.lucene" % "lucene-analysis-common" % "10.3.0",
            // PDF extraction + HTTP + JSON
            "org.apache.pdfbox" % "pdfbox" % "3.0.5",
            "com.softwaremill.sttp.client3" %% "core"  % "3.11.0",
            "com.softwaremill.sttp.client3" %% "circe" % "3.11.0",
            "io.circe" %% "circe-generic" % "0.14.14",
            "io.circe" %% "circe-parser"  % "0.14.14",
            "ch.qos.logback" % "logback-classic" % "1.5.18"
        )
    )
