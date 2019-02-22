package com.asp.testload

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.TableName
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import org.apache.hadoop.hbase.client.Scan
import scala.collection.JavaConverters._

object HBASEReadOther {
  
  def PrepareStartRowkey(eqId:String):Array[Byte] = {
    val eq_id_last=eqId.substring(eqId.length()-1,eqId.length())
    //val Key1 = Random.nextInt(9).toByte
    val Key1=eq_id_last.toByte
    
    val byte1 = new ByteArrayOutputStream();
		val data = new DataOutputStream(byte1);
		data.writeByte(Key1);
				data.writeLong(eqId.toLong);
				data.writeLong("0".toLong);
				data.writeLong("0".toLong);
		
		byte1.toByteArray()
  }
  
   def PrepareStopRowkey(eqId:String):Array[Byte] = {
    val eq_id_last=eqId.substring(eqId.length()-1,eqId.length())
    //val Key1 = Random.nextInt(9).toByte
    val Key1=eq_id_last.toByte
    
    val byte1 = new ByteArrayOutputStream();
		val data = new DataOutputStream(byte1);
		data.writeByte(Key1);
				data.writeLong(eqId.toLong);
				data.writeLong(Long.MaxValue);
				data.writeLong(Long.MaxValue);
		
		byte1.toByteArray()
  }
   
   def retbyte(full:Array[Byte],start:Int, count:Int) : Array[Byte] = {
		var retByte = Array.ofDim[Byte](count);
		  var i = start
		  var j = 0
		  while(i < (start + count)){
			  retByte(j) = full(i)
			  i = i + 1
			  j = j + 1
		  }
		return retByte;
	}
   
   def ExtractRowkey(rowKey : Array[Byte]) {
    val random = rowKey(0)
		val	devid = retbyte(rowKey, 1, 4)
		val	conid = retbyte(rowKey, 5, 4)
		val	revtimestamp = retbyte(rowKey, 9, 8)

		val	id1 = random.toString

		val	id2 = Bytes.toInt(devid)
		
		val	id3 = Bytes.toInt(conid)

		val	id4 = Bytes.toLong(revtimestamp)
		  
		  //println(id1+","+id2+","+id3+","+id4)
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
    //val data = spark.sparkContext.textFile("C:\\Users\\vignesh.i\\Documents\\samplefiles\\readdata\\").toDS()
    val data = spark.sparkContext.textFile("/testdata/readfilesnew/").toDS()
    
     data.foreachPartition { partition =>
      if (!partition.isEmpty) {

       

        val zkQuorum = "hibench.centralindia.cloudapp.azure.com"
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", zkQuorum)
        conf.set("hbase.zookeeper.property.clientPort", "2181")
        //conf.set("hbase.rootdir", "/hbase1")
        val connection = ConnectionFactory.createConnection(conf)
        val table = connection.getTable(TableName.valueOf(Bytes.toBytes("ASPLOAD3")))
                   val scan = new Scan()

        partition.foreach { mydata =>
           println("mydata",mydata)
           scan.setStartRow(PrepareStartRowkey(mydata))
           scan.setStopRow(PrepareStopRowkey(mydata))
          
           val result = table.getScanner(scan)
           val result_itr = result.asScala
           var count = 0
           for(ind_res <- result_itr)
           {
             count = count+1
             ExtractRowkey(ind_res.getRow)
             val f1 = Bytes.toString(ind_res.getValue(Bytes.toBytes("ASP"),Bytes.toBytes("F1")))
             
           }
                        println("count ",count)

           result.close
        }
        table.close()
        connection.close()
        }
      }
    
    spark.close()
  }
  
}