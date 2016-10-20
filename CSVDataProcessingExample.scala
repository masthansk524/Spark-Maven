package com.masthan.spark.practice.sparkexamples

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object CSVDataProcessingExample {
	
  
  
  def main(args:Array[String]) {
    
    val conf = new SparkConf().setAppName("Dividents").setMaster("local");
    val sc = new SparkContext(conf);
    
    val SQLContext = new org.apache.spark.sql.SQLContext(sc);
    
    val csvload = SQLContext.jsonFile("/home/projectone/spark/input/csvone.csv");
    csvload.printSchema();
    
    csvload.registerTempTable("persondata");
    SQLContext.sql("select * from persondata");
    	
	
    
  }
  
}