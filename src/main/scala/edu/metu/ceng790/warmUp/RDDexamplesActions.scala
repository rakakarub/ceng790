package edu.metu.ceng790.warmUp

import org.apache.spark.sql.SparkSession

object RDDexamplesActions {
  def main(args: Array[String]) {
	      val spark = SparkSession.builder.appName("Spark SQL").config("spark.master", "local[*]").getOrCreate()
			  val sc = spark.sparkContext
			  val file = sc.textFile("people.txt")

			  // take(n)
			  val data = sc.parallelize(Array(('k',5),('s',3),('s',4),('p',7),('p',5),('t',8),('k',6)),3)
			  val group = data.groupByKey().take(2)
			  group.foreach(println)

			  // top()
			  val mapFile = file.map(line => (line,line.length))
			  val res = mapFile.top(2)
			  res.foreach(println)


			  // count by value
			  // The countByValue() action will return a hashmap of (K, Int) pairs with the count of each key.
			  val result= mapFile.countByValue()
        result.foreach(println)

        // reduce
        val rdd1 = sc.parallelize(List(20,32,45,62,8,5))
        val sum = rdd1.reduce(_+_)
        println(sum)

        // fold
        // The signature of the fold() is like reduce(). Besides, it takes “zero value” as input,
        // which is used for the initial call on each partition. But, the condition with zero value is that
        // it should be the identity element of that operation.
        //The key difference between fold() and reduce() is that, reduce() throws an exception for empty collection,
        // but fold() is defined for empty collection.
        val rdd = spark.sparkContext.parallelize(List(("maths", 80),("science", 90)))
        val additionalMarks = ("extra", 4)
        val sum1 = rdd.fold(additionalMarks){ (acc, marks) => val add = acc._2 + marks._2
        ("total", add)
	      }
	      println(sum1)

  }

}
