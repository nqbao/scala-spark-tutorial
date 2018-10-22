package com.sparkTutorial.commons

import org.apache.spark.sql.SparkSession

import scala.reflect.io.Directory

object Utils {
  // a regular expression which matches commas but not commas within double quotations
  val COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"

  def cleanDirectory(path : String): Unit = {
    val d = Directory(path)
    if (d.exists) {
      d.deleteRecursively()
    }
  }

  def getSpark(): SparkSession = {
    SparkSession.builder()
      .master("local[2]")
      .getOrCreate()
  }
}
