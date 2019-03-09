package edu.metu.ceng790.warmUp

import org.apache.spark.{SparkConf, SparkContext}

/** Computes an approximation to pi */
object SparkPi {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Pi").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val n = 10000
    val count = sc.parallelize(1 to n).filter { _ =>
      val x = math.random
      val y = math.random
      x*x + y*y < 1
    }.count()
    println(s"Pi is roughly ${4.0 * count / n}")
    sc.stop()
  }
}
