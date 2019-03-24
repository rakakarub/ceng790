package edu.metu.ceng790.hw2.part1

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ALSParameterTuning {

  //Find Best Model
  def findBestModel(trainingData: RDD[Rating], testData: RDD[Rating]): BestALSModel = {

    val ranks = List(8, 12)
    val lambdas = List(0.01, 1.0, 10.0)
    val iterations = List(10, 20)
    var bestRank = 0
    var bestLambda = 0.0
    var bestIteration = 0
    var finalMse = Double.MaxValue
    var counter = 1

    for (rank <- ranks; lambda <- lambdas; iteration <- iterations) {
      val currentModel : MatrixFactorizationModel = ALS.train(trainingData, rank, iteration, lambda)

      val mse = calculateMSE(currentModel, testData)

      println("----- Combination #" + counter + " -----")
      println("MSE : " + mse)
      println("Rank : " + rank + ", Lambda : " + lambda + ", Iteration : " + iteration)
      println("-----------------------------")

      if(mse < finalMse) {
        finalMse = mse
        bestRank = rank
        bestIteration = iteration
        bestLambda = lambda
      }
      counter += 1
    }
    new BestALSModel(bestRank, bestLambda, bestIteration)
  }


  def calculateMSE(model: MatrixFactorizationModel, testData: RDD[Rating]): Double = {

    val userProductPairs = testData.map(e => e match {
      case Rating(user, product, rating)
      => (user, product)
    })

    val modelResults = model.predict(userProductPairs)
    modelResults.take(10).foreach(e => println("Predicted rating : " + e.rating))

    val testDataTuple2 = testData.map(e => e match {
      case
        Rating(user, product, rating) => ((user, product), rating)
    })

    val modelResultsTuple2 = modelResults.map(e => e match {
      case
        Rating(user, product, rating) => ((user, product), rating)
    })

    val combinedModelAndTestData = testDataTuple2.join(modelResultsTuple2)

    val squaredErrors = combinedModelAndTestData.map(e => {
      val error = e._2._1 - e._2._2
      error * error
    })

    val mse = squaredErrors.sum() / squaredErrors.count()

    mse
  }

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
    val dataRaw = sc.textFile(dataSetHomeDir + "ratings.csv")
    val header = dataRaw.first()
    val dataWithNoHeader = dataRaw.filter(e => e.equals(header) == false)

    //LOAD RDD ratings
    val ratings = dataWithNoHeader.map(line => line.split(',') match {
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

    avgRatingPerUser.take(1000).foreach(e => println(e.user + ", :::: " + e.rating))
    //Split data 9:1
    val splittedRatings = avgRatingPerUser.randomSplit(Array(0.9, 0.1), 12345)
    val trainingRatings = splittedRatings(0)
    val testRatings = splittedRatings(1)


    val bestModel = findBestModel(trainingRatings, testRatings)
    println("Best Lambda : " + bestModel.lambda)
    println("Best Rank : " + bestModel.rank)
    println("Best Iteration : " + bestModel.iteration)
  }
}
