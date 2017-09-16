package com.csu.cs535.assignment

import org.apache.spark.sql.SparkSession

object HitsImplementation {


  def main(args: Array[String]): Unit ={


    val spark = SparkSession.builder.appName("HITSImplementation").master("local").getOrCreate()
    val sortedTitle = spark.read.textFile("/s/chopin/b/grad/bbkstha/CS535/ProgrammingAssignment/Wikipedia Data/titles-sorted.txt")
                            .rdd.zipWithIndex()
                              .map{case (l,i) =>(i+1).toString+" "+l}
    println(sortedTitle.first())

    /*Get the query from the user.
    * Issues: Multiple strings
    *         Non alphabetical character
    *         Numeric values*/
    val queryString = "Nepal"

    val rootSet = sortedTitle.filter(l => l.contains(queryString))
    println(rootSet)
   // println(rootSet.toString())
   // println(rootSet.toString().split(" "))




    spark.stop()



  }

}

