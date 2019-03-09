package edu.metu.ceng790.warmUp

import org.apache.spark.sql.SparkSession

object RDDexamples {
  def main(args: Array[String]) {
	      val spark = SparkSession.builder.appName("Spark SQL").config("spark.master", "local[*]").getOrCreate()
			  val sc = spark.sparkContext
			  val data = sc.textFile("people.txt")

			  // map
			  val mapFile = data.map( line => (line,line.length) )
			  mapFile.map(f => f._2).foreach(println)

			  // flat map
			  val flatmapFile = data.flatMap( lines => lines.split(",") )
        flatmapFile.foreach(println)

        // filter
        val filterFile = data.flatMap(lines => lines.split(",")).filter(value => value=="Andy")
        println(filterFile.count())  // count is an action

        // union
        val rdd1 = sc.parallelize(Seq((1,"jan",2016),(3,"nov",2014),(16,"feb",2014)))
        val rdd2 = sc.parallelize(Seq((5,"dec",2014),(17,"sep",2015)))
        val rdd3 = sc.parallelize(Seq((6,"dec",2011),(16,"may",2015)))
        val rddUnion = rdd1.union(rdd2).union(rdd3)
        rddUnion.foreach(println)

        // intersection
        val rdd4 =sc.parallelize(Seq((1,"jan",2016),(3,"nov",2014), (16,"feb",2014)))
        val rdd5 =sc.parallelize(Seq((5,"dec",2014),(1,"jan",2016)))
        val comman = rdd4.intersection(rdd5)
        comman.foreach(println)

        // distinct
        val rdd = sc.parallelize(Seq((1,"jan",2016),(3,"nov",2014),(16,"feb",2014),(3,"nov",2014)))
        val result = rdd.distinct()
        println(result.collect().mkString(", "))
        // The action collect() is the common and simplest operation that returns our entire RDDs content to driver program.


        // group by key
        val dat = sc.parallelize(Array(('k',5),('s',3),('s',4),('p',7),('p',5),('t',8),('k',6)),3)
        val group = dat.groupByKey().collect()
        group.foreach(println)


        // reduce by key
        val words = Array("one","two","two","four","five","six","six","eight","nine","ten")
        val d = sc.parallelize(words).map(w => (w,1)).reduceByKey(_+_)
        d.foreach(println)


       //sort by key
        val datas = sc.parallelize(Seq(("maths",52), ("english",75), ("science",82), ("computer",65), ("maths",85)))
        val sorted = datas.sortByKey()
        sorted.foreach(println)

        // join
        val data1 = sc.parallelize(Array(('A',1),('b',2),('c',3)))
        val data2 =sc.parallelize(Array(('A',4),('A',6),('b',7),('c',3),('c',8)))
        val res = data1.join(data2)
        println(res.collect().mkString(","))
  }

}
