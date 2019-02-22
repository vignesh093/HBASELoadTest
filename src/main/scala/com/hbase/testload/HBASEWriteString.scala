package com.hbase.testload


import java.text.SimpleDateFormat
import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.ConnectionFactory
import java.util.TimeZone
import org.apache.hadoop.hbase.client.Put
import org.apache.spark.sql.SparkSession
import java.util.Date
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.TableName
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import scala.util.Random
import java.util.concurrent.ThreadLocalRandom


object HBASEWriteString {
  
    def PrepareRowKey(eqId: String, conId: String, RevTime: String): Array[Byte] = {
      
    val eq_id_last=eqId.substring(1,1)
    //val Key1 = Random.nextInt(9).toByte
    val Key1=eq_id_last.toByte
    
    val byte1 = new ByteArrayOutputStream();
    val data = new DataOutputStream(byte1);

    data.writeByte(Key1)
    data.writeLong(eqId.toLong);
    data.writeLong(conId.toLong);
    //data.writeLong(getReversedTimeStampMillisecondsNano(RevTime));
    data.writeLong(RevTime.toLong);

    val p = byte1.toByteArray();
    p
  }
def getCurrentDateInFormat() = {
    org.joda.time.DateTime.now.withZone(org.joda.time.DateTimeZone.UTC).toString("yyyy-MM-dd HH:mm:ss.SSSSSSSSS")
  }
  
  def getReversedTimeStampMillisecondsNano(inputDate: String) = {
    //With the given timestamp generate reversetimestamp with nanoseconds,required for creating HBASE row key
    var decimalPart, miliSec, nanoSec = ""
    if (inputDate.contains(".")) {
      decimalPart = inputDate.split('.')(1);
    }

    if (decimalPart.length() >= 3) {
      nanoSec = decimalPart.substring(3)
      miliSec = decimalPart.substring(0, 3)
    }

    if (miliSec == "") {
      miliSec = "000"
    }
    if (nanoSec == "") {
      nanoSec = "000000"
    }

    val reversedTime = scala.Int.MaxValue - nanoSec.padTo(6, '0').toInt

    reversedTime
  }
  def main(args: Array[String]) {
    val CF_COLUMN_NAME = Bytes.toBytes("ASP")
    val spark = SparkSession
      .builder
      .appName("HBASEWrite")
      //.master("local[*]")
      .getOrCreate()

    spark.sqlContext.setConf("spark.sql.shuffle.partitions", "8")
    
    
    import spark.implicits._

    //val content = spark.sparkContext.wholeTextFiles("/testdata/files/samplefiletoload.csv",8)
    
    //val data = spark.read.csv("/testdata/files").as[String]
    //val data = spark.sparkContext.textFile("C:\\Users\\vignesh.i\\Documents\\samplefiles\\samplefiletoload.csv").toDS()
    val data = spark.sparkContext.textFile("/testdata/files/").toDS()
    //("/testdata/files").as[String]

    //val data = content.toDS().map(x => x._2.split("\n")).flatMap { record => record }
    
    //println("Hello number of partitions ",data.rdd.getNumPartitions)
    data.foreachPartition { partition =>
      if (!partition.isEmpty) {

       

        val zkQuorum = "hibench.centralindia.cloudapp.azure.com"
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", zkQuorum)
        conf.set("hbase.zookeeper.property.clientPort", "2181")
        val connection = ConnectionFactory.createConnection(conf)
        val table = connection.getTable(TableName.valueOf(Bytes.toBytes("ASPLOAD3")))
          //var count1 = 1

        var PutList = new ArrayBuffer[Put]
        partition.foreach { mydata =>
         
          val reverse_timestamp = Long.MaxValue - System.currentTimeMillis() 

          //val rowkey = PrepareRowKey(Random.nextInt(9).toByte, 123, Random.nextInt(Integer.MAX_VALUE), reverse_timestamp)
          val timestamp = getCurrentDateInFormat()
     
          
          val rowkey = PrepareRowKey("12adc300-d758-4d69-a79e-ec9c2bb5344e",Random.nextInt(100000).toString(),Random.nextInt(100000).toString())
    
          val p = new Put(rowkey)
                     var count = 1

          for( ind_data <- mydata.split(",")){
            p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F"+count), Bytes.toBytes(ind_data.trim()))
            count = count+1;
          }
         
          //count1 = count1 + 1
          PutList += (p)

        }
        for (ind_put <- PutList) {
          table.put(ind_put)
        }
        connection.close()
        table.close()

      }
    }
    spark.stop
  }
}