package edu.metu.ceng790.hw2.part1

import org.apache.spark.mllib.recommendation.MatrixFactorizationModel

class BestALSModel(_rank: Int, _lambda: Double, _iteration: Int) {

  val rank = _rank
  val lambda = _lambda
  val iteration = _iteration
}
