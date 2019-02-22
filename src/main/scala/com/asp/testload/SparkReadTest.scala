package com.asp.testload

import org.apache.spark.sql.SparkSession

object SparkReadTest {
  def main(args:Array[String]){
    
     val spark = SparkSession
      .builder
      .appName("HBASEWrite")
      .master("local[*]")
      .getOrCreate()

    //spark.sqlContext.setConf("spark.sql.shuffle.partitions", "8")
    
    
    import spark.implicits._
    
    val data = spark.sparkContext.textFile("C:\\Users\\vignesh.i\\Documents\\samplefiles\\testsample")
    
    println("textfile partition size ",data.getNumPartitions)
    
    val data1 = spark.sparkContext.wholeTextFiles("C:\\Users\\vignesh.i\\Documents\\samplefiles\\testsample").toDF()
    
    val data2 = data1.groupBy("_1").count()
    println("wholetextfile partition size ",data2.rdd.getNumPartitions)
    
    
    
  }
}
