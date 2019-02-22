package com.asp.testload;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HBASEReadJava {
	
	public static byte[] PrepareStartRowkey(String eqId) throws IOException  {
			String eq_id_last = eqId.substring(eqId.length()-1,eqId.length());
		    byte Key1=(byte)Integer.parseInt(eq_id_last);
		    
			ByteArrayOutputStream byte1 = new ByteArrayOutputStream();
			DataOutputStream data = new DataOutputStream(byte1);
				data.writeByte(Key1);
				data.writeLong(Long.parseLong(eqId));
				data.writeLong(Long.parseLong("0"));
				data.writeLong(Long.parseLong("0"));
				//data.writeLong(Long.parseLong("0"));
				//0,74413,36868,2084300

				byte[] ret1 = byte1.toByteArray();
				return ret1;
		  }
		  
	public static byte[] PrepareStopRowkey(String eqId) throws IOException  {
		String eq_id_last = eqId.substring(eqId.length()-1,eqId.length());
	    byte Key2=(byte)Integer.parseInt(eq_id_last);
	    
		ByteArrayOutputStream byte2 = new ByteArrayOutputStream();
		DataOutputStream data = new DataOutputStream(byte2);
				/*data.writeByte(9);
				data.writeInt(74772);
				data.writeInt(36868);
				data.writeLong(4655755);*/
				
				/*data.writeByte((byte)0);
				data.writeInt((int)75999);
				data.writeInt((int)36868);
				*///data.writeLong(4655755);
		
		data.writeByte(Key2);
		data.writeLong(Long.parseLong(eqId));
		data.writeLong(Long.MAX_VALUE);
		data.writeLong(Long.MAX_VALUE);
		
				byte[] ret = byte2.toByteArray();
				return ret;
		  }
		   
	
	public static void main(String args[]) throws IOException {
		String zkQuorum = "hibench.centralindia.cloudapp.azure.com";
		        Configuration conf = HBaseConfiguration.create();
		        conf.set("hbase.zookeeper.quorum", zkQuorum);
		        conf.set("hbase.zookeeper.property.clientPort", "2181");
		        conf.set("hbase.rootdir", "/hbase1");
		        Connection connection = ConnectionFactory.createConnection(conf);
		        Table table = connection.getTable(TableName.valueOf(Bytes.toBytes("ASPLOAD3")));
		                   Scan scan = new Scan();
		                   
		                   scan.setStartRow(PrepareStartRowkey("31245"));
		                   scan.setStopRow(PrepareStopRowkey("31245"));
		                   
		                   ResultScanner result = table.getScanner(scan);
		                   int count = 0;
		                   
		                   for(Result res : result) {
		                	   System.out.println(Bytes.toString(res.getValue(Bytes.toBytes("ASP"), Bytes.toBytes("F1"))));
		                	   count++;
		                   }
		                   
		                   System.out.println("count "+count);
		                		   
		                   result.close();
		                   table.close();
		                   connection.close();
		                   
	}
}
