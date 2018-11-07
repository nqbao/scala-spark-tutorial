package com.sparkTutorial.pairRdd.filter

import com.sparkTutorial.commons.Utils

object AirportsNotInUsaProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text;
       generate a pair RDD with airport name being the key and country name being the value.
       Then remove all the airports which are located in United States and output the pair RDD to out/airports_not_in_usa_pair_rdd.text

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located,
       IATA/FAA code, ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:

       ("Kamloops", "Canada")
       ("Wewak Intl", "Papua New Guinea")
       ...
     */

    Utils.cleanDirectory("out/airports_not_in_usa_pair_rdd.text")

    val spark = Utils.getSpark()
    val sc = spark.sparkContext

    val rdd = sc.textFile("in/airports.text")
      .map(line => (
        line.split(Utils.COMMA_DELIMITER)(1), line.split(Utils.COMMA_DELIMITER)(3)
      ))
      .filter({
        case (_, country) => country != "\"United States\""
      })

    rdd.saveAsTextFile("out/airports_not_in_usa_pair_rdd.text")
  }
}
