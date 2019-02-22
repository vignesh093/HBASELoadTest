package com.hbase.testload

import java.io.ByteArrayOutputStream
import java.io.DataOutputStream

import scala.util.Random

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.client.Mutation
import org.apache.hadoop.hbase.client.Durability

case class csvdata1(_c1: Option[String], _c2: Option[String], _c3: Option[String], _c4: Option[String], _c5: Option[String], _c6: Option[String], _c7: Option[String], _c8: Option[String], _c9: Option[String], _c10: Option[String], _c11: Option[String], _c12: Option[String], _c13: Option[String], _c14: Option[String], _c15: Option[String], _c16: Option[String], _c17: Option[String], _c18: Option[String], _c19: Option[String], _c20: Option[String], _c21: Option[String], _c22: Option[String], _c23: Option[String], _c24: Option[String], _c25: Option[String], _c26: Option[String], _c27: Option[String], _c28: Option[String], _c29: Option[String], _c30: Option[String], _c31: Option[String], _c32: Option[String], _c33: Option[String], _c34: Option[String], _c35: Option[String], _c36: Option[String], _c37: Option[String], _c38: Option[String], _c39: Option[String], _c40: Option[String], _c41: Option[String], _c42: Option[String], _c43: Option[String], _c44: Option[String], _c45: Option[String], _c46: Option[String], _c47: Option[String], _c48: Option[String], _c49: Option[String], _c50: Option[String], _c51: Option[String], _c52: Option[String], _c53: Option[String], _c54: Option[String], _c55: Option[String], _c56: Option[String], _c57: Option[String], _c58: Option[String], _c59: Option[String], _c60: Option[String], _c61: Option[String], _c62: Option[String], _c63: Option[String], _c64: Option[String], _c65: Option[String], _c66: Option[String], _c67: Option[String], _c68: Option[String], _c69: Option[String], _c70: Option[String], _c71: Option[String], _c72: Option[String], _c73: Option[String], _c74: Option[String], _c75: Option[String], _c76: Option[String], _c77: Option[String], _c78: Option[String], _c79: Option[String], _c80: Option[String], _c81: Option[String], _c82: Option[String], _c83: Option[String], _c84: Option[String], _c85: Option[String], _c86: Option[String], _c87: Option[String], _c88: Option[String], _c89: Option[String], _c90: Option[String], _c91: Option[String], _c92: Option[String], _c93: Option[String], _c94: Option[String], _c95: Option[String], _c96: Option[String], _c97: Option[String], _c98: Option[String], _c99: Option[String], _c100: Option[String], _c101: Option[String], _c102: Option[String], _c103: Option[String], _c104: Option[String], _c105: Option[String], _c106: Option[String], _c107: Option[String], _c108: Option[String], _c109: Option[String], _c110: Option[String], _c111: Option[String], _c112: Option[String], _c113: Option[String], _c114: Option[String], _c115: Option[String], _c116: Option[String], _c117: Option[String], _c118: Option[String], _c119: Option[String], _c120: Option[String], _c121: Option[String])

object HBASEClientWrite {

  def PrepareRowKey(eqId: String, conId: String, RevTime: String): Array[Byte] = {

    val eq_id_last = eqId.substring(eqId.length() - 1, eqId.length())

    val Key1 = eq_id_last.toByte

    val byte1 = new ByteArrayOutputStream();
    val data = new DataOutputStream(byte1);

    data.writeByte(Key1)
    data.writeLong(eqId.toLong);
    data.writeLong(conId.toLong);

    data.writeLong(RevTime.toLong);

    val p = byte1.toByteArray();
    p
  }
  def getCurrentDateInFormat() = {
    org.joda.time.DateTime.now.withZone(org.joda.time.DateTimeZone.UTC).toString("yyyy-MM-dd HH:mm:ss.SSSSSSSSS")
  }

  def getReversedTimeStampMillisecondsNano(inputDate: String) = {

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

    val data = spark.read.format("csv").load("/testdata/readfilesoct26/samplefiletoload_new1.csv").as[csvdata1]

    println("Hello number of partitions ", data.rdd.getNumPartitions)
    data.foreachPartition { partition =>
      if (!partition.isEmpty) {

        val zkQuorum = "hibench.centralindia.cloudapp.azure.com"
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", zkQuorum)
        conf.set("hbase.zookeeper.property.clientPort", "2181")
        val connection = ConnectionFactory.createConnection(conf)

        val mutator = connection.getBufferedMutator(TableName.valueOf("ASPLOAD3"))

        partition.foreach { mydata =>

          val reverse_timestamp = Long.MaxValue - System.currentTimeMillis()

          val timestamp = getCurrentDateInFormat()

          val rowkey = PrepareRowKey("31245", Random.nextInt(100000).toString(), Random.nextInt(100000).toString())

          val p = new Put(rowkey)
          
          p.setDurability(Durability.SKIP_WAL)
          
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F1"), Bytes.toBytes(mydata._c1.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F2"), Bytes.toBytes(mydata._c2.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F3"), Bytes.toBytes(mydata._c3.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F4"), Bytes.toBytes(mydata._c4.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F5"), Bytes.toBytes(mydata._c5.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F6"), Bytes.toBytes(mydata._c6.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F7"), Bytes.toBytes(mydata._c7.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F8"), Bytes.toBytes(mydata._c8.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F9"), Bytes.toBytes(mydata._c9.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F10"), Bytes.toBytes(mydata._c10.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F11"), Bytes.toBytes(mydata._c11.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F12"), Bytes.toBytes(mydata._c12.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F13"), Bytes.toBytes(mydata._c13.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F14"), Bytes.toBytes(mydata._c14.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F15"), Bytes.toBytes(mydata._c15.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F16"), Bytes.toBytes(mydata._c16.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F17"), Bytes.toBytes(mydata._c17.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F18"), Bytes.toBytes(mydata._c18.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F19"), Bytes.toBytes(mydata._c19.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F20"), Bytes.toBytes(mydata._c20.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F21"), Bytes.toBytes(mydata._c21.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F22"), Bytes.toBytes(mydata._c22.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F23"), Bytes.toBytes(mydata._c23.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F24"), Bytes.toBytes(mydata._c24.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F25"), Bytes.toBytes(mydata._c25.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F26"), Bytes.toBytes(mydata._c26.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F27"), Bytes.toBytes(mydata._c27.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F28"), Bytes.toBytes(mydata._c28.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F29"), Bytes.toBytes(mydata._c29.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F30"), Bytes.toBytes(mydata._c30.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F31"), Bytes.toBytes(mydata._c31.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F32"), Bytes.toBytes(mydata._c32.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F33"), Bytes.toBytes(mydata._c33.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F34"), Bytes.toBytes(mydata._c34.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F35"), Bytes.toBytes(mydata._c35.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F36"), Bytes.toBytes(mydata._c36.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F37"), Bytes.toBytes(mydata._c37.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F38"), Bytes.toBytes(mydata._c38.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F39"), Bytes.toBytes(mydata._c39.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F40"), Bytes.toBytes(mydata._c40.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F41"), Bytes.toBytes(mydata._c41.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F42"), Bytes.toBytes(mydata._c42.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F43"), Bytes.toBytes(mydata._c43.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F44"), Bytes.toBytes(mydata._c44.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F45"), Bytes.toBytes(mydata._c45.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F46"), Bytes.toBytes(mydata._c46.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F47"), Bytes.toBytes(mydata._c47.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F48"), Bytes.toBytes(mydata._c48.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F49"), Bytes.toBytes(mydata._c49.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F50"), Bytes.toBytes(mydata._c50.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F51"), Bytes.toBytes(mydata._c51.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F52"), Bytes.toBytes(mydata._c52.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F53"), Bytes.toBytes(mydata._c53.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F54"), Bytes.toBytes(mydata._c54.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F55"), Bytes.toBytes(mydata._c55.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F56"), Bytes.toBytes(mydata._c56.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F57"), Bytes.toBytes(mydata._c57.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F58"), Bytes.toBytes(mydata._c58.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F59"), Bytes.toBytes(mydata._c59.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F60"), Bytes.toBytes(mydata._c60.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F61"), Bytes.toBytes(mydata._c61.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F62"), Bytes.toBytes(mydata._c62.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F63"), Bytes.toBytes(mydata._c63.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F64"), Bytes.toBytes(mydata._c64.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F65"), Bytes.toBytes(mydata._c65.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F66"), Bytes.toBytes(mydata._c66.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F67"), Bytes.toBytes(mydata._c67.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F68"), Bytes.toBytes(mydata._c68.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F69"), Bytes.toBytes(mydata._c69.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F70"), Bytes.toBytes(mydata._c70.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F71"), Bytes.toBytes(mydata._c71.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F72"), Bytes.toBytes(mydata._c72.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F73"), Bytes.toBytes(mydata._c73.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F74"), Bytes.toBytes(mydata._c74.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F75"), Bytes.toBytes(mydata._c75.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F76"), Bytes.toBytes(mydata._c76.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F77"), Bytes.toBytes(mydata._c77.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F78"), Bytes.toBytes(mydata._c78.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F79"), Bytes.toBytes(mydata._c79.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F80"), Bytes.toBytes(mydata._c80.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F81"), Bytes.toBytes(mydata._c81.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F82"), Bytes.toBytes(mydata._c82.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F83"), Bytes.toBytes(mydata._c83.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F84"), Bytes.toBytes(mydata._c84.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F85"), Bytes.toBytes(mydata._c85.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F86"), Bytes.toBytes(mydata._c86.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F87"), Bytes.toBytes(mydata._c87.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F88"), Bytes.toBytes(mydata._c88.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F89"), Bytes.toBytes(mydata._c89.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F90"), Bytes.toBytes(mydata._c90.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F91"), Bytes.toBytes(mydata._c91.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F92"), Bytes.toBytes(mydata._c92.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F93"), Bytes.toBytes(mydata._c93.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F94"), Bytes.toBytes(mydata._c94.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F95"), Bytes.toBytes(mydata._c95.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F96"), Bytes.toBytes(mydata._c96.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F97"), Bytes.toBytes(mydata._c97.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F98"), Bytes.toBytes(mydata._c98.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F99"), Bytes.toBytes(mydata._c99.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F100"), Bytes.toBytes(mydata._c100.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F101"), Bytes.toBytes(mydata._c101.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F102"), Bytes.toBytes(mydata._c102.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F103"), Bytes.toBytes(mydata._c103.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F104"), Bytes.toBytes(mydata._c104.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F105"), Bytes.toBytes(mydata._c105.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F106"), Bytes.toBytes(mydata._c106.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F107"), Bytes.toBytes(mydata._c107.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F108"), Bytes.toBytes(mydata._c108.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F109"), Bytes.toBytes(mydata._c109.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F110"), Bytes.toBytes(mydata._c110.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F111"), Bytes.toBytes(mydata._c111.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F112"), Bytes.toBytes(mydata._c112.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F113"), Bytes.toBytes(mydata._c113.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F114"), Bytes.toBytes(mydata._c114.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F115"), Bytes.toBytes(mydata._c115.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F116"), Bytes.toBytes(mydata._c116.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F117"), Bytes.toBytes(mydata._c117.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F118"), Bytes.toBytes(mydata._c118.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F119"), Bytes.toBytes(mydata._c119.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F120"), Bytes.toBytes(mydata._c120.getOrElse("")))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F121"), Bytes.toBytes(mydata._c121.getOrElse("")))

          mutator.mutate(p)

        }

        connection.close()

      }
    }
    spark.stop
  }
}
