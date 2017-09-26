package com.csu.cs535.assignment

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object HitsImplementation {

  def main(args: Array[String]): Unit ={

    val execStart = System.currentTimeMillis()
    val queryString =args.apply(0)
    val rootSetCount = args.apply(1).toInt
    val check = args.apply(2)
    val iter = args.apply(3).toInt

    val spark = SparkSession.builder.appName("HITSImplementation")
      .master("spark://boise:31720")
      .getOrCreate()
    import spark.implicits._
    val sortedTitle = spark.read.textFile("hdfs://boise:31701/titles.txt")
      .rdd.zipWithIndex().map{case (l,i) =>((i+1), l)}
      .partitionBy(new HashPartitioner(100))
      .persist()
    val linkSorted = spark.read
      .textFile("hdfs://boise:31701/links.txt")
      .rdd.map(x => (x.split(":")(0).toLong, x.split(":")(1)))
      .partitionBy(new HashPartitioner(100))
      .persist()
    val linkForEach = linkSorted.flatMapValues(y=> y.trim.split(" +")).mapValues(x=>x.toLong)
    //Search query
    val rootSetRDD = sortedTitle.filter(f=> f._2.toLowerCase.contains(queryString.toLowerCase))
    //Get appropriate sample; expect 5 sample count
    val sampleFraction = if (rootSetRDD.count()>rootSetCount) (rootSetCount.toDouble/rootSetRDD.count()) else 1.0
    val sampledRootSetRDD = rootSetRDD.sample(false, sampleFraction, 200000)
    sampledRootSetRDD.coalesce(1).saveAsTextFile("hdfs://boise:31701/Sep26Root.txt")
    val outLinkSet = sampledRootSetRDD.join(linkForEach).mapValues(x=>x._2)//.map(x=>(x._1, x._2._2))
    val linkForInLink = linkForEach.map(_.swap)//.map(x=>(x._2,x._1))	    
    val inLinkSet = sampledRootSetRDD.join(linkForInLink).map(x => (x._2._2, x._1))
    
        //test
	    //val inLinkSetNotSwap = sampledRootSetRDD.join(linkForInLink)
      //val inLinkForOuter = outLinkSet.join(inLinkSet.map(_.swap)).map(x=>x._2._2, x._1)
	    //val linkFromInLinkToRootToOutlink = linkForEach.join(inLinkForOuter).map{case(x,(y,z)) => if((y-z)==0) (x,y) else None }
	    //val linkFromOutlinkOfRootToInlinkToRoot = linkForEach.join(linkFromInLinkToRootToOutlink.map(_.swap)).map{case(x,(y,z)=> if((y-z)==0) 	//(x,y) else None }
	    //val allOutLinks = outLinkSet.union(inLinkSet).union(linkFromInLinkToRootToOutlink).union(linkFromOutlinkOfRootToInlinkToRoot)
    
    


    val allOutLinks = outLinkSet.union(inLinkSet)
    //val allOutLinks = outLinkSet.union(inLinkSet)
    allOutLinks.coalesce(1).saveAsTextFile(("hdfs://boise:31701/Sep26BaseOutLink.txt"))
    if(check=="T"){
      val allInLinks = allOutLinks.map(_.swap)//.map(x=>(x._2, x._1))
      var hubScore = allOutLinks.map(x=>(x._1, 1.0)).distinct()
      var authScore = allInLinks.map(x=> (x._1, 1.0)).distinct()
      //var tempHub = hubScore
      //var tempAuth = authScore
      //Iterate 50 times to update hub and authority scores
      for(i<-0 until iter){
        val tempHubScore = allInLinks.join(authScore).map(x=>(x._2._1,x._2._2)).reduceByKey((x,y)=>x+y)
        hubScore = normalizer(tempHubScore, spark)
        val tempAuthScore = allOutLinks.join(hubScore).map(x=>(x._2._1,x._2._2)).reduceByKey((x,y)=>x+y)
        authScore = normalizer(tempAuthScore, spark)

        //authScore.join(tempAuth).map{case(x, (y,z)=> x, (absolute(z-y)))}.foreach(f=>accum.add(f._2))
        //if (accum.value<1) break;
      }
      //Output
      val topHub = spark.sparkContext.parallelize(hubScore.sortBy(_._2, false).takeOrdered(10))
      val topHubWithTitle = topHub.join(sortedTitle).map(x=>(x._1, x._2._1, x._2._2)).sortBy(_._2, false)
      val topAuth = spark.sparkContext.parallelize(authScore.sortBy(_._2, false).takeOrdered(10))
      val topAuthWithTitle = topAuth.join(sortedTitle).map(x=>(x._1, x._2._1, x._2._2)).sortBy(_._2, false)
      topHubWithTitle.coalesce(1).saveAsTextFile("hdfs://boise:31701/Sept26TopHub.txt")
      topAuthWithTitle.coalesce(1).saveAsTextFile("hdfs://boise:31701/Sept26TopAuth.txt")
    }

    val execTime = (System.currentTimeMillis() - execStart)/60000.0
    println("The execution time is: "+execTime)

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

