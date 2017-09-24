package com.csu.cs535.assignment

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object HitsImplementation {


  def main(args: Array[String]): Unit ={

    val spark = SparkSession.builder.appName("HITSImplementation").master("local").getOrCreate()
    import spark.implicits._
    val sortedTitle = spark.read.textFile("/home/bbkstha/CSU/I/Big Data/PA/HITS/wikipedia data/sample/title.txt")
                            .rdd.zipWithIndex().map{case (l,i) =>((i+1), l)}
    val linkSorted = spark.read
      .textFile("/home/bbkstha/CSU/I/Big Data/PA/HITS/wikipedia data/sample/links.txt")
      .rdd.map(x => (x.split(":")(0).toLong, x.split(":")(1))).distinct()

    val splitLink = linkSorted.flatMapValues(y=> y.trim.split(" +"))
    val linkForEach = splitLink.mapValues(y => y.toLong)

    /*Get the query from the user.
    * Issues: Multiple strings
    *         Non alphabetical character
    *         Numeric values*/

    val queryString = "Nepal" //args[0]

    val rootSetRDD = sortedTitle.filter(f=> f._2.contains(queryString))
    //Get appropriate sample
    val sampleFraction = if (rootSetRDD.count()>5) (5.0/rootSetRDD.count()) else 1.0
    val sampledRootSetRDD = rootSetRDD.sample(false, sampleFraction, 200000)

    //sampledRootSetRDD.toDF().show()
    //linkForEach.toDF().show()
    val outLinkSet = sampledRootSetRDD.join(linkForEach).map(x=>(x._1, x._2._2))

    //outLinkSet.toDF().show()

    val linkForInLink = linkForEach.map(x=>(x._2,x._1))
    val inLinkSet = sampledRootSetRDD.join(linkForInLink).map(x => (x._2._2, x._1))

    //inLinkSet.toDF().show()

    val allOutLinks = outLinkSet.union(inLinkSet)

    //allOutLinks.toDF().show()

    val allInLinks = allOutLinks.map(x=>(x._2, x._1))
    //allInLinks.toDF().show()

    var hubScore = allOutLinks.map(x=>(x._1, 1.0)).distinct()
    var authScore = allInLinks.map(x=> (x._1, 1.0)).distinct()

    //hubScore.toDF().show()
    //Update auth and hub scores
    for(i<-0 to 2){
      authScore = allOutLinks
        .join(hubScore)
        .map(x=>(x._2._1,x._2._2))
        .reduceByKey((x,y)=>x+y)

      authScore = normalizer(authScore, spark)
      authScore.toDF().show()

      hubScore = allInLinks
        .join(authScore)
        .map(x=>(x._2._1,x._2._2))
        .reduceByKey((x,y)=>x+y)
      hubScore = normalizer(hubScore, spark)
      hubScore.toDF().show()
    }

    //hubScore.toDF().show()

    //Output
    authScore.toDF().show()
//    val topAuthTitleIndex = spark.sparkContext.parallelize(authScore.takeOrdered(10))
//    val topAuthTitle = topAuthTitleIndex.join(sortedTitle).toDF()
//    topAuthTitle.show()

//    val topHubTitleIndex = spark.sparkContext.parallelize(hubScore.takeOrdered(10))
//    val topHubTitle = topHubTitleIndex.join(sortedTitle).toDF()
//    topHubTitle.show()

    spark.stop()
  }

  def normalizer(inputRDD: RDD[(Long, Double)], sparkSession: SparkSession): RDD[(Long, Double)] = {

    val accum = sparkSession.sparkContext.doubleAccumulator("My Accumulator")
    inputRDD.foreach(f=> accum.add(f._2))
    val sum = accum.value
    val temp = inputRDD.map(x =>(x._1, x._2/sum))
    return temp
  }

}

