package com.sparkTutorial.util

import scala.reflect.io.Directory

object Util {
  def cleanDirectory(path : String): Unit = {
    val d = Directory(path)
    if (d.exists) {
      d.deleteRecursively()
    }
  }
}
