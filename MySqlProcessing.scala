package com.masthan.spark.practice.sparkexamples


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD
import java.sql.DriverManager
//import org.apache.spark.sql._
//import org.apache.spark.sql.types._


object MySqlProcessing {
  def main(args:Array[String]) {
	  val conf = new SparkConf().setAppName("Dividents").setMaster("local");
	  val sc = new SparkContext(conf);
	  
	 // val sqlContext = new org.apache.spark.sql.SQLContext(sc);
	  
//	  val hc = new org.apache.spark.sql.hive.HiveContext(sc);
	  
	  val url="jdbc:mysql://localhost:3306/mysql"
	  val username = "root"
	  val password = "hadoop"
	  
	  Class.forName("com.mysql.jdbc.Driver").newInstance
	        
	  val myRDD = new JdbcRDD( sc, () =>
DriverManager.getConnection(url,username,password) ,
"select first_name,last_name,gender from person limit ?, ?",
1, 5, 2, r => r.getString("last_name") + ", " + r.getString("first_name"))	  
	   println(myRDD.count);	   
	   myRDD.foreach(println);      
	        myRDD.saveAsTextFile("hdfs://localhost:9000/spark/sparkoutputs/mysqlprocess3")
  }
}
/*
// Mysql Data processing for ********
$ spark-submit --driver-class-path
 /home/projectone/work/hive-0.10.0/lib/mysql-connector-java-5.1.18-bin.jar
--class com.masthan.spark.practice.sparkexamples.MySqlProcessing
--master local /home/projectone/Desktop/JARS/mysqldata_spark.jar
(or)
$spark-submit 
--jars /home/projectone/work/hive-0.10.0/lib/mysql-connector-java-5.1.18-bin.jar 
--class com.masthan.spark.practice.sparkexamples.MySqlProcessing 
--master local /home/projectone/Desktop/JARS/mysqldata_spark.jar
(or)
$spark-submit 
--driver-class-path 
/home/projectone/work/hive-0.10.0/lib/mysql-connector-java-5.1.18-bin.jar 
--jars /home/projectone/work/hive-0.10.0/lib/mysql-connector-java-5.1.18-bin.jar
 
  --class com.masthan.spark.practice.sparkexamples.MySqlProcessing 
--master local /home/projectone/Desktop/JARS/mysqldata_spark.jar
*/

