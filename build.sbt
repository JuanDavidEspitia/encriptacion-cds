name := "encriptacion-cds"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.3.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % "2.0.0",
  "com.crealytics" %% "spark-excel" % "0.8.3",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided")