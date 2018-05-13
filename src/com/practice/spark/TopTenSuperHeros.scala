package com.practice.spark

import org.apache.spark.SparkContext
import org.apache.log4j._

object TopTenSuperHeros {
  
  def countCoOccurences(line: String) = {
    var elements = line.split("\\s+")
    ( elements(0).toInt, elements.length - 1 )
  }
  
  // Function to extract hero ID -> hero name tuples (or None in case of failure)
  def parseNames(line: String) : Option[(Int, String)] = {
    var fields = line.split('\"')
    if (fields.length > 1) {
      return Some(fields(0).trim().toInt, fields(1))
    } else {
      return None // flatmap will just discard None results, and extract data from Some results.
    }
  }
  
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]", "MostPopularSuperhero")   
    val names = sc.textFile("D:\\Study\\SparkScala\\SparkScala\\Marvel-names.txt")
    val namesRdd = names.flatMap(parseNames)
    val lines = sc.textFile("D:\\Study\\SparkScala\\SparkScala\\Marvel-graph.txt")
    val pairings = lines.map(countCoOccurences)
    val totalFriendsCount = pairings.reduceByKey((x,y) => (x+y))
    val flippedCount = totalFriendsCount.map(x => (x._2, x._1))
    
    val topTen = flippedCount.top(10);
    
    for(top <- topTen){
      var popularName = namesRdd.lookup(top._2)(0).split("/")(0)
       println(s"\'$popularName\' is in the top 10 popular superhero with ${top._1} co-appearances.")
    }
    
  }
  
  
}