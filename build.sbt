name := "FloyoML"

version := "0.1"

scalaVersion := "2.12.10"

resolvers += "jitpack" at "https://jitpack.io"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-mllib_2.12" % "3.0.0",
  "org.apache.spark" % "spark-streaming_2.12" % "3.0.0",
  "com.sksamuel.elastic4s" % "elastic4s-http_2.12" % "6.7.7",
  "com.sksamuel.elastic4s" % "elastic4s-spray-json_2.12" % "6.7.7",
  "com.beust" % "jcommander" % "1.78",
  "junit" % "junit" % "4.13" % Test
)