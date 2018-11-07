package com.sparkTutorial.pairRdd.groupbykey

import com.sparkTutorial.commons.Utils

object AirportsByCountryProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text,
       output the the list of the names of the airports located in each country.

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:

       "Canada", List("Bagotville", "Montreal", "Coronation", ...)
       "Norway" : List("Vigra", "Andenes", "Alta", "Bomoen", "Bronnoy",..)
       "Papua New Guinea",  List("Goroka", "Madang", ...)
       ...
     */

    Utils.cleanDirectory("out/airports_by_country.text")

    val spark = Utils.getSpark()
    val sc = spark.sparkContext

    val raw = sc.textFile("in/airports.text")
      .map(line => (
        line.split(Utils.COMMA_DELIMITER)(3), line.split(Utils.COMMA_DELIMITER)(1)
      ))

    val rdd = raw.groupByKey()
        .mapValues(l => l.toList)

    rdd.saveAsTextFile("out/airports_by_country.text")
  }
}
