package com.csu.cs535.assignment

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object HitsImplementation {

  def main(args: Array[String]): Unit ={
    val spark = SparkSession.builder.appName("HITSImplementation").master("local").getOrCreate()
    import spark.implicits._
    val sortedTitle = spark.read.textFile("/home/bbkstha/CSU/I/Big Data/PA/HITS/wikipedia data/titles-sorted.txt") //get HDFS links
                            .rdd.zipWithIndex().map{case (l,i) =>((i+1), l)}
    val linkSorted = spark.read
      .textFile("/home/bbkstha/CSU/I/Big Data/PA/HITS/wikipedia data/links-simple-sorted.txt") //get HDFS links
      .rdd.map(x => (x.split(":")(0).toLong, x.split(":")(1))).distinct()
    val splitLink = linkSorted.flatMapValues(y=> y.trim.split(" +"))
    val linkForEach = splitLink.mapValues(y => y.toLong)
    /*Get the query from the user.
    * Issues: Multiple strings
    *         Non alphabetical character
    *         Numeric values*/
    val queryString = "Nepal" //args[0]
    val rootSetRDD = sortedTitle.filter(f=> f._2.toLowerCase.contains(queryString.toLowerCase))
    //Get appropriate sample; expect 50 sample count
    val sampleFraction = if (rootSetRDD.count()>5) (5.0/rootSetRDD.count()) else 1.0
    val sampledRootSetRDD = rootSetRDD.sample(false, sampleFraction, 200000)
    val outLinkSet = sampledRootSetRDD.join(linkForEach).map(x=>(x._1, x._2._2))
    val linkForInLink = linkForEach.map(x=>(x._2,x._1))
    val inLinkSet = sampledRootSetRDD.join(linkForInLink).map(x => (x._2._2, x._1))
    val allOutLinks = outLinkSet.union(inLinkSet).persist()
    val allInLinks = allOutLinks.map(x=>(x._2, x._1)).persist()
    var hubScore = allOutLinks.map(x=>(x._1, 1.0)).distinct()
    var authScore = allInLinks.map(x=> (x._1, 1.0)).distinct()
    //Iterate 50 times to update hub and authority scores
    for(i<-0 until 2){
      val tempHubScore = allInLinks.join(authScore).map(x=>(x._2._1,x._2._2)).reduceByKey((x,y)=>x+y)
      val tempAuthScore = allOutLinks.join(hubScore).map(x=>(x._2._1,x._2._2)).reduceByKey((x,y)=>x+y)
      hubScore = normalizer(tempHubScore, spark)
      authScore = normalizer(tempAuthScore, spark)
    }
    //Output
    val topHub = spark.sparkContext.parallelize(hubScore.sortBy(_._2, false).takeOrdered(3))
    val topHubWithTitle = topHub.join(sortedTitle).map(x=>(x._1, x._2._1, x._2._2)).sortBy(_._2, false).toDF()
    val topAuth = spark.sparkContext.parallelize(authScore.sortBy(_._2, false).takeOrdered(3))
    val topAuthWithTitle = topAuth.join(sortedTitle).map(x=>(x._1, x._2._1, x._2._2)).sortBy(_._2, false).toDF()
    topHubWithTitle.show()
    topAuthWithTitle.show()

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

