package com.practice.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._


object TotalAmoutPerCustomer {
  
  def parseLines(lines:String) = {
    val fields = lines.split(",");
    val customerID = fields(0).toInt
    val amount = fields(2).toFloat;
    (customerID, amount)
  }
  
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR);
    val sc =  new SparkContext("local[*]", "TotalAmoutPerCustomer")
    val lines = sc.textFile("D:/Study/SparkScala/SparkScala/customer-orders.csv");
    val parsedLines = lines.map(parseLines);
    val count = parsedLines.reduceByKey((x,y) => (x+y));
    val result = count.collect();
    /*Unsorted data*/
    result.foreach(println);
    /*Sorted Data*/
    val resultFlipped = count.map(x => (x._2, x._1))
    val sortedCount = resultFlipped.sortByKey();
    println("----------Sorted Output------------")
    sortedCount.collect().foreach(println)
  }
  
  
}