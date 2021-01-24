name := "mail_spark_3"

version := "0.1"

scalaVersion := "2.12.8"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-hive
libraryDependencies += "org.apache.spark" %% "spark-hive" % "3.0.1" % "provided"

// https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.0.1" % "provided"

// https://mvnrepository.com/artifact/com.nvidia/rapids-4-spark
libraryDependencies += "com.nvidia" %% "rapids-4-spark" % "0.2.0"

// https://mvnrepository.com/artifact/ai.rapids/cudf
libraryDependencies += "ai.rapids" % "cudf" % "0.14"

