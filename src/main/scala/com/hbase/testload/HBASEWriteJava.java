package com.hbase.testload;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HBASEWriteJava {
	
	  /*public static byte[] getTSRowKey(eqId: String, varId: String, RevTime: String) = {
			    //Prepare row key with format of timeseriesorevent_equipmentid_variableid_ReverseTS_ReverseTSwithnano
			    
			    val eq_id_last=eqId.substring(eqId.length()-1,eqId.length())
			    val Key1=eq_id_last.toByte
			    
			    val Key2: Long = (eqId).toLong
			    val Key3: Long = (varId).toLong
			    val Key4: Long = getReversedTimeStampMillisecondsWihtoutNano(RevTime)
			    val Key5: Int = getReversedTimeStampMillisecondsNano(RevTime)

			    val byte1 = new ByteArrayOutputStream();
			    val data = new DataOutputStream(byte1);

			    data.writeByte(Key1);
			    data.writeLong(Key2);
			    data.writeLong(Key3);
			    data.writeLong(Key4); //Rev TS
			    data.writeInt(Key5);

			    val p = byte1.toByteArray();
			    p
			  }*/
	  
	public static byte[] PrepareRowkey(byte rando, int devid, int conid) throws IOException  {
		ByteArrayOutputStream byte1 = new ByteArrayOutputStream();
		DataOutputStream data = new DataOutputStream(byte1);
			data.writeByte(rando);
			data.writeInt(devid);
			data.writeInt(conid);
			//data.writeLong(revtime);
			//0,74413,36868,2084300

			return byte1.toByteArray();
	  }
	
	public static void main(String args[]) throws IOException {
		String zkQuorum = "hibench.centralindia.cloudapp.azure.com";
		        Configuration conf = HBaseConfiguration.create();
		        conf.set("hbase.zookeeper.quorum", zkQuorum);
		        conf.set("hbase.zookeeper.property.clientPort", "2181");
		        conf.set("hbase.rootdir", "/hbase1");
		        Connection connection = ConnectionFactory.createConnection(conf);
		        Table table = connection.getTable(TableName.valueOf(Bytes.toBytes("NEWTEST")));
		        Random rand = new Random();
		        byte[] rowkey = PrepareRowkey((byte)0, rand.nextInt(75999), 12686);
		        
		        Put p = new Put(rowkey);
		        p.addColumn(Bytes.toBytes("TEST"), Bytes.toBytes("F1"), Bytes.toBytes("hi".trim()));
		        p.addColumn(Bytes.toBytes("TEST"), Bytes.toBytes("F2"), Bytes.toBytes("hiii".trim()));
		        p.addColumn(Bytes.toBytes("TEST"), Bytes.toBytes("F3"), Bytes.toBytes("hiiii".trim()));
		        table.put(p);
		        
		        table.close();
		        connection.close();
		        		
	}
}
