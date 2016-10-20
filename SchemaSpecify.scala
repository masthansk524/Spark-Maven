package com.masthan.spark.practice.sparkexamples

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SchemaSpecify {
	def main(args:Array[String]) {
	  val conf = new SparkConf().setAppName("Programmatically Specify Schema").setMaster("local");
	  val sc = new SparkContext(conf);
	  
	  //specifying schema
	  val data = sc.textFile("/home/projectone/dataschemaspecify");
	  
	  val schemaString = "name age";
//	  val fields = schemaString.split(" ")
//	  .map(fieldName =>  StructField(fieldName,StringType,mutable=true));
//	  
	  
	  
	}
}