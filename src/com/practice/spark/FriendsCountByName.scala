package com.practice.spark

import org.apache.log4j._
import org.apache.spark.SparkContext

object FriendsCountByName {

  /** ACTIVITY : Compute the average number of friends by first name in a social network. */
  def parselinesOnNameAndAge(line: String) = {
    val fields = line.split(",");
    val name = fields(1).toString();
    val friendsCount = fields(3).toInt;
    (name, friendsCount)
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR);
    val sc = new SparkContext("local[*]", "FriendsCountByName");
    val lines = sc.textFile("D:/Study/SparkScala/SparkScala/fakefriends.csv")

    val rdd = lines.map(parselinesOnNameAndAge)

    val totalsByAge = rdd.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

    val averagesByAge = totalsByAge.mapValues(x => x._1 / x._2)

    val results = averagesByAge.collect()

    results.sorted.foreach(println)

  }

}