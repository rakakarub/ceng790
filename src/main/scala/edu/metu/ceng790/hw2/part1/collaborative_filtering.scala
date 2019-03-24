package edu.metu.ceng790.hw2.part1

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.io.StdIn
import scala.util.Random

object collaborative_filtering {

  def main(args: Array[String]): Unit = {

    val dataSetHomeDir = "hw2_dataset/ml-20m/"

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Assignment-2_Part-1:Getting Your Own Recommendation")
      .set("spark.executor.memory", "3g")
    //Create Spark Context
    val sc = new SparkContext(conf)
    //Set log level
    sc.setLogLevel("ERROR")
    sc.setCheckpointDir("/tmp")

    //LOAD DATA
    val dataRawMovies = sc.textFile(dataSetHomeDir + "movies.csv")
    val headerMovies = dataRawMovies.first()
    val dataWithNoHeaderMovies = dataRawMovies.filter(e => e.equals(headerMovies) == false)

    val dataRawRatings = sc.textFile(dataSetHomeDir + "ratings.csv")
    val headerRatings = dataRawRatings.first()
    val dataWithNoHeaderRatings = dataRawRatings.filter(e => e.equals(headerRatings) == false)

    val ratings = dataWithNoHeaderRatings.map(line => line.split(',') match {
      case Array(user, movieID, rating, date)
      => Rating(user.toInt, movieID.toInt, rating.toDouble)
    })

    //Normalize the ratings
    val ratingsGroupByUser = ratings.groupBy(e => e.user).map(x =>
      (x._1, x._2.map(y => y.rating)))

    val userAverageRatingMap = ratingsGroupByUser.map(e => {
      (e._1, e._2.sum/e._2.size)
    }).collect().toMap

    val avgRatingPerUser = ratings.map(e => e match {
      case Rating(user, product, rating) =>
        val userAvgRatings = userAverageRatingMap.get(user).get
        Rating(user, product, rating/userAvgRatings)
    })

    val lambda = 0.01
    val rank = 8
    val iteration = 20

    //Built movies: Map[Int, String]
    val moviesRaw = dataWithNoHeaderMovies.map(line => line.split(','))
    val movies: Map[Int, String] = moviesRaw.map(e => (e(0).toInt, e(1))).collect().toMap

    //Build mostRatedMovies
    val mostRatedMovieIDs = avgRatingPerUser.map(e => e match {
      case Rating(user, product, rating) =>
        product
    }).countByValue().toSeq.sortBy(a => - (a match {
      case (movieID, ratingCount) => ratingCount
    })).take(200)

    val mostRatedMovies = mostRatedMovieIDs.map(e => {
      (e._1, movies.get(e._1).toString)
    })

    //Obtain selectedMovies: List[(Int, String)]
    val selectedMovies = Random.shuffle(mostRatedMovies).take(40)

    val userRatings: RDD[Rating] = sc.parallelize(elicitateRatings(selectedMovies))

    val userIDRatingsMap = userRatings.groupBy(e => e.user).map(x =>
      (x._1, x._2.map(y => y.rating)))

    val userIDAvgRatingMap = userIDRatingsMap.map(e => {
      (e._1, e._2.sum / e._2.size)
    }).collect().toMap

    val normalizedUserRatings = userRatings.map(e => e match  {
      case Rating(user, product, rating) =>
        val avgRating = userIDAvgRatingMap.get(user).get
        Rating(user, product, rating / avgRating)
    })

    val updatedRatings = sc.union(avgRatingPerUser, normalizedUserRatings).collect()
    val updatedRatingsRDD = sc.parallelize(updatedRatings)

    val userRatedMovieIDs = normalizedUserRatings.map(e => e.product).map(x => x).collect()

    val recomPair = moviesRaw.filter(e => userRatedMovieIDs.contains(e(0).toInt) == false)
      .map(x => {
        (0, x(0).toInt)
      })

//    avgRatingPerUser.take(50).foreach(e => println(e.user + ", ::: " + e.rating))
//    val recomPair = moviesRaw.map(x => (999999999, x(0).toInt))

    println("Total Movie Size : " + moviesRaw.count())
    println("Predicted movie size : " + recomPair.count())

    val newModel = ALS.train(updatedRatingsRDD, rank, iteration, lambda)
    val recommendations = newModel.predict(recomPair).sortBy(e => - e.rating).take(20)

    recommendations.foreach(e => println(movies.get(e.product).toString + ", predicted rating : " + e.rating))

  }

  def elicitateRatings(movies: Seq[(Int, String)]): Seq[Rating] = {
    println("It is time to RATE !!!!")

    val userRatings = movies.map(e => {
      print("Your rating for the " + e._2.toString + " : ")
      var rate = StdIn.readInt()
      Rating(0, e._1, rate)
    })
    println("Please wait for a while to get your movie recommendations :))))")
    userRatings
  }

  /** Elicitate ratings from command-line. */
  def elicitateRatingsV2(movies: Seq[(Int, String)]) = {
    println("It is time to RATE !!!!")
    val userRatings = movies.flatMap { x =>
      var ratingObject: Option[Rating] = None
        print("Your rating for the " + x._2 + " : ")
          val rate = StdIn.readInt()
            if (rate > 0) {
              ratingObject = Some(Rating(1, x._1, rate))
            }
      ratingObject
    }
    userRatings
  }
}
