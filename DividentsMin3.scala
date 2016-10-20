package com.masthan.spark.practice.sparkexamples

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object DividentsMin3 {
  
//  def procesData(key:String, value:Float) {
//	    val res = value.toList.distinct.sortWith(_<_).take(2).toArray;
//	  }
	def main(args:Array[String]) {
	  val conf = new SparkConf().setAppName("Dividents").setMaster("local");
	  val sc = new SparkContext(conf);	  
	  val data = sc.textFile("/home/projectone/nyse_dividents.csv");
	  val divdata = data.map(rec => (rec.split(",")(1),rec.split(",")(3)));
	  val grpdivdata = divdata.groupByKey();
	  // function with list and sortWith	 
	  var res2=""
	  val finaldata = grpdivdata.collect.foreach(
	  rec => {
	    val(k,v) = rec
	    val res = v.toList.distinct.sortWith(_<_).take(3).toArray;
	    println(k+" " + res.mkString(","));
	     val res1 =k+" "+res.mkString(",");
	     res2=res2+res1;	    
	   } 	  
	  );
	  val x= sc.parallelize(res2.toList)
	  x.saveAsTextFile("/home/projectone/Desktop/deivdents1");
	 
//	  res.saveAsTextFile("/home/projectone/Desktop/deivdents1");
	  
	  
	  
	  
	  
	  
	}
}