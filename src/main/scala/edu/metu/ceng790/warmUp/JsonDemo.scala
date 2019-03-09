package edu.metu.ceng790.warmUp

import org.apache.spark.sql.SparkSession

object JsonDemo {
	def main(args: Array[String]) {
		    val spark = SparkSession.builder.appName("Spark SQL").config("spark.master", "local[*]").getOrCreate()
				val sc = spark.sparkContext
				val sqlContext = new org.apache.spark.sql.SQLContext(sc)

				// A JSON dataset is pointed to by path.
				val people = sqlContext.read.json("people.json")

				// The inferred schema can be visualized using the printSchema() method.
				people.printSchema()
				// root
				//  |-- age: integer (nullable = true)
				//  |-- name: string (nullable = true)

				// Register this DataFrame as a table.
				people.createOrReplaceTempView("people")

				// SQL statements can be run by using the sql methods provided by sqlContext.
				val teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")
        teenagers.rdd.map(t => "Name: " + t(0)).collect().foreach(println)

	}
}
