name := "scala_spark_poc"

version := "0.1"

scalaVersion := "2.11.11"

libraryDependencies ++= Seq(
  //"org.twitter4j" %% "twitter4j-core" % "4.0.4",
  //"org.twitter4j" %% "twitter4j-stream" % "4.0.4",
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark" %% "spark-streaming" % "2.1.0",
  "org.apache.bahir" %% "spark-streaming-twitter" % "2.1.0",
  "joda-time" % "joda-time" % "2.9.9",
  "org.json4s" %% "json4s-native" % "3.5.3",
  "org.json4s" %% "json4s-jackson" % "3.5.3"
)