package edu.metu.ceng790.hw1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.ByteType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.LongType


object Part1 {
  def main(args: Array[String]): Unit = {

    var spark: SparkSession = null
    try {

      spark = SparkSession.builder().appName("Flickr using dataframes").config("spark.master", "local[*]").getOrCreate()

      //   * Photo/video identifier
      //   * User NSID
      //   * User nickname
      //   * Date taken
      //   * Date uploaded
      //   * Capture device
      //   * Title
      //   * Description
      //   * User tags (comma-separated)
      //   * Machine tags (comma-separated)
      //   * Longitude
      //   * Latitude
      //   * Accuracy
      //   * Photo/video page URL
      //   * Photo/video download URL
      //   * License name
      //   * License URL
      //   * Photo/video server identifier
      //   * Photo/video farm identifier
      //   * Photo/video secret
      //   * Photo/video secret original
      //   * Photo/video extension original
      //   * Photos/video marker (0 = photo, 1 = video)

      val customSchemaFlickrMeta = StructType(Array(
        StructField("photo_id", LongType, true),
        StructField("user_id", StringType, true),
        StructField("user_nickname", StringType, true),
        StructField("date_taken", StringType, true),
        StructField("date_uploaded", StringType, true),
        StructField("device", StringType, true),
        StructField("title", StringType, true),
        StructField("description", StringType, true),
        StructField("user_tags", StringType, true),
        StructField("machine_tags", StringType, true),
        StructField("longitude", FloatType, false),
        StructField("latitude", FloatType, false),
        StructField("accuracy", StringType, true),
        StructField("url", StringType, true),
        StructField("download_url", StringType, true),
        StructField("license", StringType, true),
        StructField("license_url", StringType, true),
        StructField("server_id", StringType, true),
        StructField("farm_id", StringType, true),
        StructField("secret", StringType, true),
        StructField("secret_original", StringType, true),
        StructField("extension_original", StringType, true),
        StructField("marker", ByteType, true)))

      val originalFlickrMeta = spark.sqlContext.read
        .format("csv")
        .option("delimiter", "\t")
        .option("header", "false")
        .schema(customSchemaFlickrMeta)
        .load("flickrSample.txt")

      // YOUR CODE HERE

      //Create Spark Context
      val sc = spark.sparkContext
      //Set log level
      sc.setLogLevel("ERROR")

      // 1 - Select fields identifier(photo_id), GPS coord(longitude, latitude), license
      println("==== Question - 1 -> Select fields (photo_id, longitude, latitude, license)")
      println()

      originalFlickrMeta.createOrReplaceTempView("pictures")
      val selectedFields = spark.sql("SELECT photo_id, longitude, latitude, license FROM pictures");
      selectedFields.collect().foreach(println)

      println("===============")
      println

      // 2 - Create a DataFrame with interesting pictures
      println("==== Question - 2 -> Select interesting pictures ====")
      println

      val interestingPicturesDF = spark.sql("SELECT photo_id, longitude, latitude, license " +
        "FROM pictures WHERE license is not null AND longitude != -1.0 AND latitude != -1.0")

      println("===============")
      println

      // 3 - Display execution plan
      println("==== Question - 3 -> Display execution plan ====")
      println

      interestingPicturesDF.explain()

      println("===============")
      println

      // 4 - Display the data of this pictures (show())
      println("==== Question - 4 -> Show interesting pictures ====")
      println

      interestingPicturesDF.show()

      println("===============")
      println()

      // 5 - Select pictures with license NonDerivative
      println("==== Question - 5 -> Select pictures with license NonDerivative ====")
      println

      interestingPicturesDF.createOrReplaceTempView("interestingPictures")

      val flickrLicenses = spark.sqlContext.read
        .format("csv")
        .option("delimiter", "\t")
        .option("header", "true")
        .load("flickrLicense.txt")

      flickrLicenses.show()

      flickrLicenses.createOrReplaceTempView("licences")

      val picturesNonDerivative = spark.sql("SELECT ip.* " +
        "FROM interestingPictures AS ip " +
        "JOIN licences AS l ON l.Name = ip.license " +
        "WHERE l.NonDerivative = true")

      picturesNonDerivative.explain()
      picturesNonDerivative.show()

      println("===============")
      println

      // 6 - Cache and re-examine
      println("==== Question - 6 -> Cache the interesting pictures and re-examine explain() ====")
      println

      interestingPicturesDF.cache()
      picturesNonDerivative.explain()

      println("===============")
      println

      // 7 - save as a file
      println("==== Question - 7 -> Save as a csv file ====")
      println

      picturesNonDerivative.write
        .format("csv")
        .option("delimiter", "\t")
        .option("header", "true")
        .save("result")

      println("===============")
    } catch {
      case e: Exception => throw e
    } finally {
      spark.stop()
    }
    println("done")
  }
}