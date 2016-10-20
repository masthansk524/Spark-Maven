package com.masthan.spark.practice.sparkexamples

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Depttop2SparkExample {
  def main(args:Array[String]) {
	  var conf = new SparkConf().setAppName("Spark Sal calculate").setMaster("local");
	  var sc = new SparkContext(conf);
	  
	  val loaddata = sc.textFile("/home/projectone/sparkexample2");
	  val rows = loaddata.map(w => (w.split(",")(4),w.split(",")(2)));
	  val sortSalRDD=rows.sortByKey();
	  val groupBydeptRDD=sortSalRDD.groupByKey();
	  groupBydeptRDD.collect.foreach(rec => 
	    {
	      val(k,v) = rec
	      val res = v.toList.distinct.sortWith(_>_).take(2).toArray
	      println(k + "  "+res.mkString(",") );
	      
	    }
	  )
	  
	  
	  
	  
  }

}