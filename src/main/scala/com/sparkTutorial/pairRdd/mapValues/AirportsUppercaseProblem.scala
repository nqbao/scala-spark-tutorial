package com.sparkTutorial.pairRdd.mapValues

import com.sparkTutorial.commons.Utils

object AirportsUppercaseProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text, generate a pair RDD with airport name
       being the key and country name being the value. Then convert the country name to uppercase and
       output the pair RDD to out/airports_uppercase.text

       Each row of the input file contains the following columns:

       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:

       ("Kamloops", "CANADA")
       ("Wewak Intl", "PAPUA NEW GUINEA")
       ...
     */

    Utils.cleanDirectory("out/airports_uppercase.text")

    val spark = Utils.getSpark()
    val sc = spark.sparkContext

    val rdd = sc.textFile("in/airports.text")
      .map(line => (
        line.split(Utils.COMMA_DELIMITER)(1), line.split(Utils.COMMA_DELIMITER)(3)
      ))
      .mapValues(country => country.toUpperCase)

    rdd.saveAsTextFile("out/airports_uppercase.text")
  }
}
