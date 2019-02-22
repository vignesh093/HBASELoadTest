package com.hbase.testload

import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.TableName

object HBASEWriteScala {
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
  
   def getTSRowKey(eqId: String, varId: String, RevTime: String) = {
    //Prepare row key with format of timeseriesorevent_equipmentid_variableid_ReverseTS_ReverseTSwithnano
    
    val eq_id_last=eqId.substring(eqId.length()-1,eqId.length())
    val Key1=eq_id_last.toByte
    
    val Key2: Long = (eqId).toLong
    val Key3: Long = (varId).toLong
    //val Key4: Long = getReversedTimeStampMillisecondsWihtoutNano(RevTime)
    val Key4: Int = getReversedTimeStampMillisecondsNano(RevTime)

    val byte1 = new ByteArrayOutputStream();
    val data = new DataOutputStream(byte1);

    data.writeByte(Key1);
    data.writeLong(Key2);
    data.writeLong(Key3);
    data.writeLong(Key4); //Rev TS

    val p = byte1.toByteArray();
    p
  }
  
  def main(args:Array[String]){
    
    val zkQuorum = "hibench.centralindia.cloudapp.azure.com"
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", zkQuorum)
        conf.set("hbase.zookeeper.property.clientPort", "2181")
        val connection = ConnectionFactory.createConnection(conf)
        val table = connection.getTable(TableName.valueOf(Bytes.toBytes("ASPLOAD2")))

        
    val timestamp = getCurrentDateInFormat()
    val rowKeyForVolume = getTSRowKey("13244", "2", timestamp)
    
    val p = new Put(rowKeyForVolume)
    
    p.addColumn(Bytes.toBytes("TEST"), Bytes.toBytes("F1"), Bytes.toBytes("hi".trim()));
    p.addColumn(Bytes.toBytes("TEST"), Bytes.toBytes("F2"), Bytes.toBytes("hi".trim()));
    p.addColumn(Bytes.toBytes("TEST"), Bytes.toBytes("F3"), Bytes.toBytes("hiiii".trim()));
    table.put(p);
		        
		        table.close();
		        connection.close();

  }
}