package com.practice.spark

import org.apache.log4j._;
import org.apache.spark.SparkContext

object StopwordCount {
  
    /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local", "WordCountBetterSorted")   
    
    val input = sc.textFile("D:/Study/SparkScala/SparkScala/book.txt")
    
    val stopword_input = sc.textFile("D:/Study/SparkScala/SparkScala/stopwords_en.txt");
    
    val mapStopWords = stopword_input.flatMap(x => x.split(" "));
    
    val broadcastStopWords = sc.broadcast(mapStopWords.collect.toSet)
    
    val words = input.flatMap(x => x.split("\\W+"))
    
    val lowercaseWords = words.map(x => x.toLowerCase())
    
    val filteredLowercaseWords = lowercaseWords.filter(broadcastStopWords.value.contains(_));
    
    val wordCounts = filteredLowercaseWords.map(x => (x, 1)).reduceByKey( (x,y) => x + y )
    
    val wordCountsSorted = wordCounts.map( x => (x._2, x._1) ).sortByKey()
    for (result <- wordCountsSorted) {
      val count = result._1
      val word = result._2
      println(s"$word: $count")
    }
    
  }
}