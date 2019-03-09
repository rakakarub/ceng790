package edu.metu.ceng790.warmUp

import org.apache.spark.sql.SparkSession

object CombinebyKeyExample {
	def main(args: Array[String]) {
		val spark = SparkSession.builder.appName("Spark SQL").config("spark.master", "local[*]").getOrCreate()
				val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

				// combineByKey is a general transformation whereas internal implementation of transformations
				// groupByKey, reduceByKey and aggregateByKey uses combineByKey.

				// Creating PairRDD studentRDD with key value pairs
				val studentRDD = sc.parallelize(Array(
						("Joseph", "Maths", 83), ("Joseph", "Physics", 74), ("Joseph", "Chemistry", 91),
						("Joseph", "Biology", 82), ("Jimmy", "Maths", 69), ("Jimmy", "Physics", 62),
						("Jimmy", "Chemistry", 97), ("Jimmy", "Biology", 80), ("Tina", "Maths", 78),
						("Tina", "Physics", 73), ("Tina", "Chemistry", 68), ("Tina", "Biology", 87),
						("Thomas", "Maths", 87), ("Thomas", "Physics", 93), ("Thomas", "Chemistry", 91),
						("Thomas", "Biology", 74), ("Cory", "Maths", 56), ("Cory", "Physics", 65),
						("Cory", "Chemistry", 71), ("Cory", "Biology", 68), ("Jackeline", "Maths", 86),
						("Jackeline", "Physics", 62), ("Jackeline", "Chemistry", 75), ("Jackeline", "Biology", 83),
						("Juan", "Maths", 63), ("Juan", "Physics", 69), ("Juan", "Chemistry", 64),
						("Juan", "Biology", 60)), 3)


				//Defining createCombiner, mergeValue and mergeCombiner functions
				def createCombiner = (tuple: (String, Int)) => (tuple._2.toDouble, 1)

				// It is a first aggregation step for each key
        // It will be executed when any new key is found in a partition
        // Execution is local to a partition of a node, on each individual values

		    def mergeValue = (accumulator: (Double, Int), element: (String, Int)) => (accumulator._1 + element._2, accumulator._2 + 1)

		    // Second function executes when next subsequent value is given to combiner
        // It also executes locally on each partition of a node and combines all values
        // Arguments of this function are a accumulator and a new value
        // It combines a new value in existing accumulator

		    def mergeCombiner = (accumulator1: (Double, Int), accumulator2: (Double, Int)) => (accumulator1._1 + accumulator2._1, accumulator1._2 + accumulator2._2)
        // Final function is used to combine how to merge two accumulators (i.e. combiners) of a single key
		    // across the partitions to generate final expected result
        // Arguments are two accumulators (i.e. combiners)
        // Merge results of a single key from different partitions


		    // use combineByKey for finding percentage
		    val combRDD = studentRDD.map(t => (t._1, (t._2, t._3)))
		                            .combineByKey(createCombiner, mergeValue, mergeCombiner)
		                            .map(e => (e._1, e._2._1/e._2._2))

    		//Check the Outout
    		combRDD.collect foreach println

    		// Output
    		// (Tina,76.5)
    		// (Thomas,86.25)
    		// (Jackeline,76.5)
    		// (Joseph,82.5)
    		// (Juan,64.0)
    		// (Jimmy,77.0)
    		// (Cory,65.0)
	}

}
