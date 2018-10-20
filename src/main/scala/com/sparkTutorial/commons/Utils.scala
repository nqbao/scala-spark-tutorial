package com.sparkTutorial.commons

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
}
