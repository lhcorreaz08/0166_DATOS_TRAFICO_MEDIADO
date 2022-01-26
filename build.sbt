name := "0080_DATOS_CARGUE_RE-BLT"

version := "0.1"

scalaVersion := "2.11.12"

resolvers += Resolver.bintrayRepo("spark-packages", "maven")

val sparkVersion = "2.3.2"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.2" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.3.2"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.2" % Test


libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.1.1.3.1.0.0-78" % "provided"
resolvers += ("Hortonworks repo" at "http://repo.hortonworks.com/content/repositories/releases/").withAllowInsecureProtocol(true)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}