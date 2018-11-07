package com.sparkTutorial.pairRdd.sort

import com.sparkTutorial.commons.Utils


object SortedWordCountProblem {

    /* Create a Spark program to read the an article from in/word_count.text,
       output the number of occurrence of each word in descending order.

       Sample output:

       apple : 200
       shoes : 193
       bag : 176
       ...
     */

  def main(args: Array[String]): Unit = {
    val spark = Utils.getSpark()
    val sc = spark.sparkContext

    Utils.cleanDirectory("out/word_count.text")

    val words = sc.textFile("in/word_count.text")
      .filter(line => !line.isEmpty)
      .flatMap(line => line.split(" "))

    val count = words.map(w => (w, 1))
        .reduceByKey((x, y) => x+y)
        .map({ case (x,y) => (y,x) })
        .sortByKey(ascending = false)

    count.saveAsTextFile("out/word_count.text")
  }
}

