package se.kth.spark.lab1.task6

import org.apache.spark.ml.linalg.{ Matrices, Vector, Vectors, DenseVector, DenseMatrix }

object VectorHelper {
  def dot(v1: Vector, v2: Vector): Double = {
    //seriously
    (v1.toArray,v2.toArray).zipped.map(_ * _).fold(0.0)(_ + _)
  }

  def dot(v: Vector, s: Double): Vector = {
    new DenseVector(v.toArray.map(_ * s))
  }

  def sum(v1: Vector, v2: Vector): Vector = {
    new DenseVector((v1.toArray, v2.toArray).zipped.map(_ + _))
  }

  def fill(size: Int, fillVal: Double): Vector = {
    new DenseVector(Array.fill(size) { fillVal })}
}