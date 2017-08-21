name:= "GeneralReports"
 
version := "2.4.1"
 
scalaVersion := "2.10.6"
 
libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "1.6.0",
    "org.apache.hadoop" % "hadoop-common" % "2.4.1",
    "org.scalaj" % "scalaj-http_2.10" % "2.3.0",
    "org.json4s" % "json4s-jackson_2.10" % "3.2.11"
)
