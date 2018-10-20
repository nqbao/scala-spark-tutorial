package com.sparkTutorial.rdd.airports

import org.apache.spark.sql.SparkSession
import com.sparkTutorial.util._

object AirportsInUsaProblem {
  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text, find all the airports which are located in United States
       and output the airport's name and the city's name to out/airports_in_usa.text.

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:
       "Putnam County Airport", "Greencastle"
       "Dowagiac Municipal Airport", "Dowagiac"
       ...
     */

    Util.cleanDirectory("out/airports_in_usa")

    val spark = SparkSession.builder()
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext

    val raw = sc.textFile("in/airports.text")
    val out = raw.map(line => {
        line.split(",")
      })
      .filter(line => line(3) == "\"United States\"")
      .map(line => s"${line(2)},${line(3)}")

    out.saveAsTextFile("out/airports_in_usa")
  }
}
