package edu.metu.ceng790.hw1

import org.apache.spark.sql.SparkSession

import org.apache.spark.rdd.RDD

object Part2 {
  def main(args: Array[String]): Unit = {
    var spark: SparkSession = null
    try {
      spark = SparkSession.builder().appName("Flickr using dataframes").config("spark.master", "local[*]").getOrCreate()
      val originalFlickrMeta: RDD[String] = spark.sparkContext.textFile("flickrSample.txt")
      
      // YOUR CODE HERE

      //Create Spark Context
      val sc = spark.sparkContext
      //Set log level
      sc.setLogLevel("ERROR")

      // 1 - Display the 5 lines of RDD and display the count
      println("==== Question - 1 -> Display the first 5 lines and line count ====")
      print("First 5 Line : ")
      println
      originalFlickrMeta.take(5).foreach(println)
      println

      println("Line number : " + originalFlickrMeta.count())

      println("===============")
      println

      // 2 - Transform RDD[String] to RDD[Picture]
      println("==== Question - 2 -> Convert RDD[String] to RDD[Picture]")
      println

      val allPictures : RDD[Picture] = originalFlickrMeta.map(rawPicture => rawPicture.split("\t")).map(splittedLine => new Picture(splittedLine))
      val validPictures : RDD[Picture] = allPictures.map(p => p).filter(picture => picture.hasTags && picture.hasValidCountry)

      println("First 5 picture : ")
      println

      validPictures.take(5).foreach(p => println(p.toString))

      println("===============")
      println

      // 3 - GroupBy country
      println("==== Question - 3 -> GroupBy Country")
      println

      val groupedByCountry = validPictures.groupBy(p => p.c)
      println(groupedByCountry.first())

      println("===============")
      println

      // 4 - RDD pairs (Country, ListOfTags)
      println("==== Question - 4 -> (Country, ListOfTags) list of tags is flatten")
      println

      val countryWithTags = groupedByCountry.map(e => (e._1, e._2.flatten(t => t.userTags)))
      countryWithTags.foreach(println)

      println("===============")
      println

      // 5 - Find Tag Frequency
      println("==== Question - 5 -> Create a Map(tag, numberOfTag). Final object is (Country, Map(Tag, NumberOfTag))")
      println

      val tagWithFrequency = countryWithTags.map(e => (e._1, e._2.groupBy(t => t).mapValues(listOfTags => listOfTags.size)))
      tagWithFrequency.foreach(println)

      println("===============")

    } catch {
      case e: Exception => throw e
    } finally {
      spark.stop()
    }
    println("done")
  }
}