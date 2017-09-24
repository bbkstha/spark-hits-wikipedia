package com.csu.cs535.assignment

import org.apache.spark.sql.SparkSession

object TestPairRDD {

  def main(args: Array[String]): Unit ={


    val spark = SparkSession.builder.appName("TestPairRDD").master("local").getOrCreate()
    import spark.implicits._


    val testPairRDD = spark.sparkContext.parallelize(List((1, Seq(10,100)), (2, Seq(20,200,2000)), (3, Seq(30,300,3000))))
    //testPairRDD.foreach(println)
    //testPairRDD.keys.foreach(println)
    //val newTuple = testPairRDD.join(testPairRDD)
    //newTuple.foreach(println)

    val contrib = testPairRDD.flatMapValues(x=>x.productIterator)
    contrib.foreach(println)

    val pageScore = testPairRDD.mapValues(s=>1.0)

    pageScore.foreach(println)

    //
    val test = testPairRDD.join(pageScore).flatMap{
      case(id, (testPairRDD, score)) =>
        testPairRDD.map(link => (link, score+1))
    }
    test.keys.foreach(println)
    //val result = test.reduceByKey((x,y)=>x+y).mapValues(v=>v+1)
    //result.foreach(println)

    spark.stop()

  }


}
