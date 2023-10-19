package com.qwery.runtime.datatypes

/**
 * Matrix Vector
 */
object Vector {

  type Vector = Array[Double]

  def apply(values: Double*): Vector = Array[Double](values: _*)

  def dotProduct(vectorA: Vector, vectorB: Vector): Double = {
    vectorA.zip(vectorB).map { case (a, b) => a * b }.sum
  }

  def normalizeVector(vector: Vector): Vector = {
    val norm = math.sqrt(dotProduct(vector, vector))
    vector.map(_ / norm)
  }

  final implicit class RichMatrixVector(val vectorA: Vector) extends AnyVal {

    def dotProduct(vectorB: Vector): Double = Vector.dotProduct(vectorA, vectorB)

    def normalizeVector: Vector = Vector.normalizeVector(vectorA)

  }

}
