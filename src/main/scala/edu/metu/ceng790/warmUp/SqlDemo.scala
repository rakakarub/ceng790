package edu.metu.ceng790.warmUp

import org.apache.spark.sql._

// Define the schema using a case class.
case class Person(name: String, age: Int)
	  
object SqlDemo {
	def main(args: Array[String]) {
	  val spark = SparkSession.builder.appName("Spark SQL").config("spark.master", "local[*]").getOrCreate()
		val sc = spark.sparkContext
		sc.setLogLevel("ERROR")

		val sqlContext = new org.apache.spark.sql.SQLContext(sc)
		import sqlContext.implicits._

	  // Create an RDD of Person objects and register it as a table.
	  val people = sc.textFile("people.txt").map(_.split(","))
	  .map(p => Person(p(0), p(1).trim.toInt)).toDF()

    people.createOrReplaceTempView("people");

	  // SQL statements can be run by using the sql methods provided by sqlContext.
    val olders = sqlContext.sql("SELECT name FROM people WHERE age >= 20")
	  // The results of SQL queries are SchemaRDDs and support all the normal RDD operations.
	  // The columns of a row in the result can be accessed by ordinal.
    olders.map(t => "Name: " + t(0)).collect().foreach(println)
    
    // The following is the same as
    // 'SELECT name FROM people WHERE age >= 13 AND age <= 19'!
    val teenagers2 = people.where('age >= 13).where('age <= 19).select('name)
    
    teenagers2.map(t => "Name: " + t(0)).collect().foreach(println)
	}
}