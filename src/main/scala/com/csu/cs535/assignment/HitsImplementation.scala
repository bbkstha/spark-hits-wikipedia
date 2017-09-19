package com.csu.cs535.assignment

import org.apache.spark.sql.SparkSession

object HitsImplementation {

  case class Title (index: Int, title: String)


  def main(args: Array[String]): Unit ={


    val spark = SparkSession.builder.appName("HITSImplementation").master("local").getOrCreate()
    import spark.implicits._


    val sortedTitle = spark.read.textFile("/s/chopin/b/grad/bbkstha/CS535/ProgrammingAssignment/Wikipedia Data/titles-sorted.txt")
                            .rdd.zipWithIndex().map{case (l,i) =>((i+1), l)}//.toDS() //.map(x,y => (x.split(" ")(0), x)).toDS()

    val linkSorted = spark.read.textFile("/s/chopin/b/grad/bbkstha/CS535/ProgrammingAssignment/Wikipedia Data/links-simple-sorted.txt").rdd
                      //.rdd.zipWithIndex().map{case (l,i) =>((i+1), l)}//.toDS() //.map(x,y => (x.split(" ")(0), x)).toDS()

    /*Get the query from the user.
    * Issues: Multiple strings
    *         Non alphabetical character
    *         Numeric values*/

    val queryString = "Nepal"
    val res = sortedTitle.filter(f=> f._2.contains(queryString)).keys

    println(res)

    //val res = spark("SELECT * FROM sortedTitle")

    //val rootSet = sortedTitle.select("_1").show(10)//filter(_1 = queryString)

    //val rootSet = spark.sql("Select index from 'sortedTitle' where title = queryString")
    //println(rootSet)
   // println(rootSet.toString())
   // println(rootSet.toString().split(" "))




    spark.stop()



  }

}

