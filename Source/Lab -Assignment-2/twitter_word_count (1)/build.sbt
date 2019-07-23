name := "NetworkWordCount"

scalaVersion := "2.11.8"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark" %% "spark-sql" % "2.1.0",
  "org.apache.spark" % "spark-streaming_2.11" % "2.1.0",
  "org.apache.bahir" %% "spark-streaming-twitter" % "2.0.1",
  "org.graphframes" % "graphframes" % "0.7.0-spark2.4-s_2.11",
  "org.apache.spark" %% "spark-graphx" % "2.1.0"
)