name := "hellostreaming"
version := "1.0"
scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "2.1.0"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "2.1.0"

// estou usando esse cara para fazer parse de json
//libraryDependencies += "com.typesafe.play" % "play-json_2.10" % "2.4.8"

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.10" % "2.0.0"
libraryDependencies += "org.json4s" % "json4s-native_2.10" % "3.5.1"
