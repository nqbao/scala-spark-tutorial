package com.sparkTutorial.rdd.sumOfNumbers

import com.sparkTutorial.commons.Utils

object SumOfNumbersProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
       print the sum of those numbers to console.

       Each row of the input file contains 10 prime numbers separated by spaces.
     */

    val spark = Utils.getSpark()
    val sc = spark.sparkContext

    val numbers = sc.textFile("in/prime_nums.text")
      .flatMap(line => line.trim().split("\\s+"))
      .take(100)

    // numbers.foreach(println)

    val total = numbers
      .map(r => r.toInt)
      .reduce((a, b) => a + b)

    println(total)
  }
}
