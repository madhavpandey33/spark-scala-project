package com.practice.spark

import org.apache.log4j._
import org.apache.spark.SparkContext
import scala.math.max;

object FilterByMaxTemp {
  
  def parseLines(lines:String) ={
    val fields = lines.split(",");
    val stationID = fields(0);
    val labels = fields(2)
    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (stationID, labels, temperature)
  }
  
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR);
    val sc = new SparkContext("local[*]", "FilterByMaxTemp");
    val lines = sc.textFile("D:/Study/SparkScala/SparkScala/1800.csv");
    val parsedLines = lines.map(parseLines);
    val maxTemp = parsedLines.filter(x => x._2 == "TMAX");
    val stationTemMap = maxTemp.map(x => (x._1,x._3));
    val stationMaxTemp = stationTemMap.reduceByKey((x,y) => max(x,y))
    val result =  stationMaxTemp.collect();
    
    for(rs <- result.sorted) {
       val station = rs._1
       val temp = rs._2
       val formattedTemp = f"$temp%.2f F"
       println(s"$station maximum temperature: $formattedTemp") 
    }
  }
  
}