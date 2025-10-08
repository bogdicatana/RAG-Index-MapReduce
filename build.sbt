import sbt.Keys.libraryDependencies

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.4.0"

lazy val root = (project in file("."))
    .settings(
        name := "cs441hw1",
        Compile / mainClass := Some("Main"),

        // Don't include test classes in the final JAR
        Test / test := {},

        // Merge strategy to handle duplicate META-INF files (common with logging libs)
        assembly / assemblyMergeStrategy := {
            case PathList("META-INF", "services", "java.net.spi.InetAddressResolverProvider") => MergeStrategy.discard // <--- DISCARD THE FAULTY FILE
            case PathList("META-INF", "services", xs @ _*) => MergeStrategy.concat // Keep other service files merged
            case PathList("META-INF", "spring.tooling", xs @ _*) => MergeStrategy.concat
            case PathList("META-INF", xs @ _*) => MergeStrategy.discard
            case "reference.conf" => MergeStrategy.concat
            case "application.conf" => MergeStrategy.concat
            case PathList(ps @ _*) if ps.last.matches(".*.RSA|.*.DSA|.*.SF") => MergeStrategy.discard
            case "package.html" => MergeStrategy.discard
            case x => MergeStrategy.first
        },
        libraryDependencies ++= Seq(
            // Retrieval index (pure JVM, cross-platform)
            "org.apache.lucene" % "lucene-core" % "10.3.0",
            "org.apache.lucene" % "lucene-analysis-common" % "10.3.0",
            "org.apache.lucene" % "lucene-queryparser" % "10.3.0",
            // PDF extraction + HTTP + JSON
            "org.apache.pdfbox" % "pdfbox" % "3.0.5",
            "com.softwaremill.sttp.client3" %% "core"  % "3.11.0",
            "com.softwaremill.sttp.client3" %% "circe" % "3.11.0",
            "io.circe" %% "circe-generic" % "0.14.14",
            "io.circe" %% "circe-parser"  % "0.14.14",
            "ch.qos.logback" % "logback-classic" % "1.5.18",
            // Config library
            "com.typesafe" % "config" % "1.4.5",
            "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.4.2",
            "org.apache.hadoop" % "hadoop-common" % "3.4.2",
            "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % "3.4.2",
//            "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.4.2" % "provided",
//            "org.apache.hadoop" % "hadoop-common" % "3.4.2" % "provided",
//            "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % "3.4.2" % "provided",
            "org.scalactic" %% "scalactic" % "3.2.19",
            "dnsjava" % "dnsjava" % "3.6.3",
            "org.scalatest" %% "scalatest" % "3.2.17" % Test
        )
    )
