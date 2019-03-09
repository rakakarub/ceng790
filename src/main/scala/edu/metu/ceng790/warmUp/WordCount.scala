package edu.metu.ceng790.warmUp

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Word Count").setMaster("local")
		val sc = new SparkContext(conf)

    val f = sc.textFile("alice.txt")
    val wc = f.flatMap(l => l.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    //wc.saveAsTextFile("wc_out")

    wc.takeOrdered(10).foreach(println)
   wc.sortBy( _._2, ascending = false ).take(5).foreach(println)
  }
}
