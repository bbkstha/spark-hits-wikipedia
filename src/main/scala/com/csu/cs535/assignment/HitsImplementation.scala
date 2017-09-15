package com.csu.cs535.assignment

import org.apache.spark.sql.SparkSession

object HitsImplementation {


  def main(args: Array[String]): Unit ={

    val logFile = "/s/chopin/b/grad/bbkstha/Downloads/spark/spark-2.2.0-bin-hadoop2.7/README.md" // Should be some file on your system
    val spark = SparkSession.builder.appName("HITSMain").master("local").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()


  }

}
