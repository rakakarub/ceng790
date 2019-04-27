package edu.metu.ceng790.hw3

import org.apache.spark._
import org.apache.spark.ml.{Model, Pipeline, PipelineModel}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit, TrainValidationSplitModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._


object Credit {
  // define the Credit Schema
  case class Credit(
    creditability: Double,
    balance: Double, duration: Double, history: Double, purpose: Double, amount: Double,
    savings: Double, employment: Double, instPercent: Double, sexMarried: Double, guarantors: Double,
    residenceDuration: Double, assets: Double, age: Double, concCredit: Double, apartment: Double,
    credits: Double, occupation: Double, dependents: Double, hasPhone: Double, foreign: Double
  )
  // function to create a  Credit class from an Array of Double
  def parseCredit(line: Array[Double]): Credit = {
    Credit(
      line(0),
      line(1) - 1, line(2), line(3), line(4), line(5),
      line(6) - 1, line(7) - 1, line(8), line(9) - 1, line(10) - 1,
      line(11) - 1, line(12) - 1, line(13), line(14) - 1, line(15) - 1,
      line(16) - 1, line(17) - 1, line(18) - 1, line(19) - 1, line(20) - 1
    )
  }
  // function to transform an RDD of Strings into an RDD of Double
  def parseRDD(rdd: RDD[String]): RDD[Array[Double]] = {
    rdd.map(_.split(",")).map(_.map(_.toDouble))
  }

  def main(args: Array[String]) {

        val spark = SparkSession.builder
          .appName("Spark SQL")
          .config("spark.master", "local[*]")
          .getOrCreate()

        val sc = spark.sparkContext
        sc.setLogLevel("ERROR")

        val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._
    // load the data into a  RDD
    val creditDF = parseRDD(sc.textFile("credit.csv"))
      .map(parseCredit)
      .toDF()
      .cache()

    // set a table name
    creditDF.createOrReplaceGlobalTempView("CreditTable")
    creditDF.printSchema()

    // set LABEL AND FEATURES columns
    val LABEL = "creditability"
    val FEATURES = Array(
      "balance",
      "duration",
      "history",
      "purpose",
      "amount",
      "savings",
      "employment",
      "instPercent",
      "sexMarried",
      "guarantors",
      "residenceDuration",
      "assets",
      "age",
      "concCredit",
      "apartment",
      "credits",
      "occupation",
      "dependents",
      "hasPhone",
      "foreign")

    // 1- create VectorAssembler
    val vectorAssembler = new VectorAssembler()
      .setInputCols(FEATURES)
      .setOutputCol("FEATURES")

    //Transform DF with VectorAssembler
    val dfWithFeatures = vectorAssembler
      .transform(creditDF)

    dfWithFeatures.printSchema()

    // 2- create StringIndexer
    val stringIndexer = new StringIndexer()
      .setInputCol(LABEL)
      .setOutputCol("LABEL")

    // Transform DF with StringIndexer
    val stringIndexerDF = stringIndexer
      .fit(dfWithFeatures)
      .transform(dfWithFeatures)

    stringIndexerDF.printSchema()

    // 3- Split data with 8:2 ratio
    val Array(trainData, testData) = stringIndexerDF.randomSplit(Array(0.800, 0.200), 12345L)
    println("Train data size : " + trainData.count())
    println("Test data size : " + testData.count())

    // 4- Train a random forest classifier with random HyperParameters
    val evaluator = new BinaryClassificationEvaluator().setLabelCol("LABEL")
    val randomForestClassifier = new RandomForestClassifier()
      .setFeatureSubsetStrategy("auto")
      .setSeed(1234)
      .setFeaturesCol("FEATURES")
      .setLabelCol("LABEL")

    // Create ParamGridBuilder
    val paramGridBuilder = new ParamGridBuilder()
      .addGrid(randomForestClassifier.maxBins, Array(25, 28, 31))
      .addGrid(randomForestClassifier.maxDepth, Array(4, 6, 8))
      .addGrid(randomForestClassifier.impurity, Array("entropy", "gini"))
      .build()

    // 5- Create and set up a pipeline

    val pipeline = new Pipeline()
      .setStages(Array(randomForestClassifier))

    // Create TrainValidationSplit
    val trainValidationSplit = new TrainValidationSplit()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGridBuilder)
      .setTrainRatio(0.75)

    val trainValidationSplitModel : TrainValidationSplitModel = trainValidationSplit.fit(trainData)
    val pipelineBestFittedModel : PipelineModel = trainValidationSplitModel.bestModel.asInstanceOf[PipelineModel]

    println("##########  Best Model's Parameter Explanation : ########## ")
    println()
    println(pipelineBestFittedModel.stages.last.explainParams())


    val pipelinePredictions = pipelineBestFittedModel.transform(testData)

    val bestAccuracy = evaluator.evaluate(pipelinePredictions)
    println("BEST ACCURACY : " + bestAccuracy)

    val trainPredictions = pipelineBestFittedModel.transform(trainData)
    val trainAccuracy = evaluator.evaluate(trainPredictions)
    println("TRAIN ACCURACY : " + trainAccuracy)

  }
}

