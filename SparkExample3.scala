package com.masthan.spark.practice.sparkexamples

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object SparkExample3 {
	def main(args:Array[String]) {
	  var conf = new SparkConf().setAppName("Spark Sal calculate").setMaster("local");
	  var sc = new SparkContext(conf);
	  
	  val loaddata = sc.textFile("/home/projectone/sparkexample2");
	  //val lines = loaddata.flatMap(line => line.split(","));
	  val rows = loaddata.map(w => (w.split(",")(4),w.split(",")(2)));
	  val deptsal = rows.reduceByKey((a,b) => (a+b));
	  println("Sum of salariess based on department");
	  deptsal.saveAsTextFile("/home/projectone/spark/output/example2");
	  
	  deptsal.collect.foreach(println);
	  println("*******************");
	  
	  val maxsaldept = rows.reduceByKey((a,b) => if (a>b) a else b );
	  println("Max Salary is");
	  maxsaldept.saveAsTextFile("/home/projectone/spark/output/examplemaxsal");
	  maxsaldept.collect.foreach(println);
	  println("************************");
	  
	  println("Min salary in dataset");
	  val minsaldept = rows.reduceByKey((a,b) => if(a<b) a else b);
	  minsaldept.saveAsTextFile("/home/projectone/spark/output/exampleminsal");
	  minsaldept.collect.foreach(println);
	  println("***********");
	  
	  /*spark-submit 
	   * --class com.masthan.spark.practice.sparkexamples.WordCount
	   *  --master local 
	   *  /home/projectone/Desktop/JARS/wordcount_spark.jar 
	   *  hdfs://localhost:9000/spark/sparkinputs/oneone.txt 
	   *  hdfs://localhost:9000/spark/sparkoutputs/wordcount_spark
	   * 
	   * 
	   * 
	   */
	   
	}
}