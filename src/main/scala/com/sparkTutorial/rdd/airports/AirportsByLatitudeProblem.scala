package com.sparkTutorial.rdd.airports

import com.sparkTutorial.commons.Utils
import org.apache.spark.sql.SparkSession

object AirportsByLatitudeProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text,  find all the airports whose latitude are bigger than 40.
       Then output the airport's name and the airport's latitude to out/airports_by_latitude.text.

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:
       "St Anthony", 51.391944
       "Tofino", 49.082222
       ...
     */

    Utils.cleanDirectory("out/airports_by_latitude.text")

    val spark = SparkSession.builder()
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext

    val raw = sc.textFile("in/airports.text")
    val out = raw.map(line => {
      line.split(Utils.COMMA_DELIMITER)
    })
      .filter(line => line(6).toFloat >= 40)
      .map(line => s"${line(1)},${line(6)}")

    out.saveAsTextFile("out/airports_by_latitude.text")
  }
}
