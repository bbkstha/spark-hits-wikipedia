package com.csu.cs535.assignment

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.util.control._

object HitsImplementation {

  def main(args: Array[String]): Unit ={

    val execStart = System.currentTimeMillis()
    val queryString =args.apply(0)
    val rootSetCount = args.apply(1).toInt
    val saveFile = args.apply(2)
    val iter = args.apply(3).toInt

    val spark = SparkSession.builder.appName("HITSImplementation")
      .master("spark://boise:31720")
      .getOrCreate()
    import spark.implicits._
    val sortedTitle = spark.read.textFile("hdfs://boise:31701/titles-sorted.txt")
      .rdd.zipWithIndex().map{case (l,i) =>((i+1).toInt, l)}
      .partitionBy(new HashPartitioner(100))
      .persist()
    val linkSorted = spark.read
      .textFile("hdfs://boise:31701/links-simple-sorted.txt")
      .rdd.map(x => (x.split(":")(0).toInt, x.split(":")(1)))
      .partitionBy(new HashPartitioner(100))
      .persist()

    val outlinkForEach = linkSorted.flatMapValues(y=> y.trim.split(" +")).mapValues(x=>x.toInt)
    //Finding Root Set
    val rootSet = sortedTitle.filter(f=> f._2.toLowerCase.contains(queryString.toLowerCase))
    //Get appropriate sample; expect 5 sample count
    val sampleFraction = if (rootSet.count()>rootSetCount) rootSetCount.toFloat/rootSet.count() else 1.0 //Scaling root set as user defined
    val sampledRootSet = rootSet.sample(false, sampleFraction, 200000)
    //Base-set Outlink Construction
    val outlinkSet = outlinkForEach.groupByKey()
    val trimedOutlinkSet = outlinkSet.map{case(x, y) => (x, if (y.size>20) y.slice(0, 19) else y)} //adding at most 20 outgoing links
    val trimedOutlink = trimedOutlinkSet.flatMapValues(x=>x)
    val baseSetOutlink = sampledRootSet.join(trimedOutlink).mapValues(x=>x._2)
    //Base-set Inlink Construction
    val inlinkForEach = outlinkForEach.map(_.swap)
    val inlinkSet = sampledRootSet.join(inlinkForEach).mapValues(x=>x._2).groupByKey()
    val trimedInlinkSet = inlinkSet.map{case(x, y) => (x, if (y.size>20) y.slice(0, 19) else y)} //adding at most 20 incoming links
    val trimedInlink = trimedInlinkSet.flatMapValues(x=>x)
    val baseSetInlink = trimedInlink.map(_.swap)
    //Base-set
    val allOutLinks = baseSetOutlink.union(baseSetInlink).persist()
    val allInLinks = allOutLinks.map(_.swap).persist()
    val baseSetLinkCount = allOutLinks.count()
    println(baseSetLinkCount)
    var hubScore = allOutLinks.map(x=>(x._1, 1.0.toFloat)).distinct()
    var authScore = allInLinks.map(x=> (x._1, 1.0.toFloat)).distinct()
    val hubCount = hubScore.count()
    val authCount = authScore.count()
    println(hubCount)
    println(authCount)
    //Iterate 40 times at most to update hub and authority scores
    val loop = new Breaks;
    loop.breakable{
        for(i<-0 to iter){
          val tempA = authScore
          val tempAuthScore = allOutLinks.join(hubScore).map(x=>(x._2._1,x._2._2)).reduceByKey((x,y)=>x+y)
          authScore = normalizer(tempAuthScore, spark)
          val tempHubScore = allInLinks.join(authScore).map(x=>(x._2._1,x._2._2)).reduceByKey((x,y)=>x+y)
          hubScore = normalizer(tempHubScore, spark)
          //Threashold Check
              if(i>25){
                if(i>=40 | threshold(tempA, authScore, spark)<0.000005) {
                  loop.break
                }
              }
        }
    }
    //Output
    val topHub = spark.sparkContext.parallelize(hubScore.takeOrdered(50)(Ordering[Double].reverse.on(_._2)))
    val topHubWithTitle = topHub.join(sortedTitle).map(x=>(x._2._1, x._2._2))
    val topAuth = spark.sparkContext.parallelize(authScore.takeOrdered(50)(Ordering[Double].reverse.on(_._2)))
    val topAuthWithTitle = topAuth.join(sortedTitle).map(x=>(x._2._1, x._2._2))
    //Save output in hdfs
    if(saveFile=="Y"){
        sampledRootSet.coalesce(1).saveAsTextFile("hdfs://boise:31701/RootSet.txt")
        topHubWithTitle.coalesce(1).saveAsTextFile("hdfs://boise:31701/Top50Hub.txt")
        topAuthWithTitle.coalesce(1).saveAsTextFile("hdfs://boise:31701/Top50Auth.txt")
    }
    //Show output as DF
    topHubWithTitle.sortBy(_._1, false).toDF().show()
    topAuthWithTitle.sortBy(_._1, false).toDF().show()
    println("The number of auth node: "+authCount)
    println("The number of hub node: "+hubCount)
    println("The number of links in baseset is: "+baseSetLinkCount)
    val execTime = (System.currentTimeMillis() - execStart)/60000.0
    println("The execution time is: "+execTime)
    spark.stop()
  }

  def normalizer(inputRDD: RDD[(Int, Float)], sparkSession: SparkSession): RDD[(Int, Float)] = {
    val accum = sparkSession.sparkContext.doubleAccumulator("My Accumulator")
    inputRDD.foreach(f=> accum.add(f._2))
    val sum = accum.value.toFloat
    val temp = inputRDD.map(x =>(x._1, x._2/sum))
    return temp
  }

  def threshold(originalRDD: RDD[(Int, Float)], updatedRDD: RDD[(Int, Float)], sparkSession: SparkSession): Double = {

    val accum = sparkSession.sparkContext.doubleAccumulator("Threshold Accumulator")
    val tempRDD =originalRDD.join(updatedRDD).mapValues(y => (y._1 - y._2).abs)
    tempRDD.foreach(f=> accum.add(f._2))
    return accum.value
  }
}