import sbtassembly.MergeStrategy

name := "HBASELoadTest"
version := "1.0"
scalaVersion := "2.11.11"
autoScalaLibrary := false


libraryDependencies ++= {
  Seq(
  "org.apache.spark" %% "spark-core" % "2.3.0",
  "org.apache.spark" %% "spark-sql" % "2.3.0",
  "org.apache.hadoop" % "hadoop-client" % "2.6.0-cdh5.15.1",
  "org.apache.hbase" % "hbase-client" % "1.2.0-cdh5.15.1",
  "org.apache.hbase" % "hbase-common" % "1.2.0-cdh5.15.1",
  "it.nerdammer.bigdata" % "spark-hbase-connector_2.10" % "1.0.3",
  // https://mvnrepository.com/artifact/org.apache.spark/spark-streaming,
  "org.apache.spark" %% "spark-streaming" % "2.3.0" % "provided",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.3.0" % "provided" 
  )
}






resolvers ++= Seq(
"Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
"Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
"MavenRepository" at "https://mvnrepository.com/"
)


assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}