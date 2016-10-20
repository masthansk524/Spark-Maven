package com.masthan.spark.practice.sparkexamples

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkDataramesExample1 {

  def main(args:Array[String]) {
	  val conf = new SparkConf().setAppName("Dividents").setMaster("local");
	  val sc = new SparkContext(conf);
	  
	  
	  
	  
  }
}