package com.asp.testload

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

case class csvdata(_c1: String, _c2: String, _c3: String, _c4: String, _c5: String, _c6: String, _c7: String, _c8: String, _c9: String, _c10: String, _c11: String, _c12: String, _c13: String, _c14: String, _c15: String, _c16: String, _c17: String, _c18: String, _c19: String, _c20: String, _c21: String, _c22: String, _c23: String, _c24: String, _c25: String, _c26: String, _c27: String, _c28: String, _c29: String, _c30: String, _c31: String, _c32: String, _c33: String, _c34: String, _c35: String, _c36: String, _c37: String, _c38: String, _c39: String, _c40: String, _c41: String, _c42: String, _c43: String, _c44: String, _c45: String, _c46: String, _c47: String, _c48: String, _c49: String, _c50: String, _c51: String, _c52: String, _c53: String, _c54: String, _c55: String, _c56: String, _c57: String, _c58: String, _c59: String, _c60: String, _c61: String, _c62: String, _c63: String, _c64: String, _c65: String, _c66: String, _c67: String, _c68: String, _c69: String, _c70: String, _c71: String, _c72: String, _c73: String, _c74: String, _c75: String, _c76: String, _c77: String, _c78: String, _c79: String, _c80: String, _c81: String, _c82: String, _c83: String, _c84: String, _c85: String, _c86: String, _c87: String, _c88: String, _c89: String, _c90: String, _c91: String, _c92: String, _c93: String, _c94: String, _c95: String)

object HBASEWrite {

  def PrepareRowKey(eqId: String, conId: String, RevTime: String): Array[Byte] = {

    val eq_id_last = eqId.substring(eqId.length() - 1, eqId.length())
    //val Key1 = Random.nextInt(9).toByte
    val Key1 = eq_id_last.toByte

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
    //val data = spark.sparkContext.textFile("/testdata/files/").toDS()
    val data = spark.read.format("csv").load("/testdata/readfilesoct26/samplefiletoload_new1.csv").as[csvdata]
    //println(data.count())
    //data.show
    //("/testdata/files").as[String]

    //val data = content.toDS().map(x => x._2.split("\n")).flatMap { record => record }

    println("Hello number of partitions ", data.rdd.getNumPartitions)
    data.foreachPartition { partition =>
      if (!partition.isEmpty) {

        val zkQuorum = "hibench.centralindia.cloudapp.azure.com"
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", zkQuorum)
        conf.set("hbase.zookeeper.property.clientPort", "2181")
        val connection = ConnectionFactory.createConnection(conf)
        //val table = connection.getTable(TableName.valueOf(Bytes.toBytes("ASPLOAD3")))
        val mutator = connection.getBufferedMutator(TableName.valueOf("ASPLOAD3"))
        //var count1 = 1

        //var PutList = new ArrayBuffer[Put]
        partition.foreach { mydata =>

          val reverse_timestamp = Long.MaxValue - System.currentTimeMillis()
          //var putcount = 0

          //val rowkey = PrepareRowKey(Random.nextInt(9).toByte, 123, Random.nextInt(Integer.MAX_VALUE), reverse_timestamp)
          val timestamp = getCurrentDateInFormat()

          val rowkey = PrepareRowKey("31245", Random.nextInt(100000).toString(), Random.nextInt(100000).toString())

          val p = new Put(rowkey)
          /*            var count = 1
          val mylist = mydata.productIterator.toList.asInstanceOf[List[String]]
          //while(count < 99){
          for(ele <- mylist) {
            p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F"+count), Bytes.toBytes))
            count = count+1;
          }

          //count1 = count1 + 1
*/
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F1"), Bytes.toBytes(mydata._c1))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F2"), Bytes.toBytes(mydata._c2))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F3"), Bytes.toBytes(mydata._c3))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F4"), Bytes.toBytes(mydata._c4))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F5"), Bytes.toBytes(mydata._c5))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F6"), Bytes.toBytes(mydata._c6))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F7"), Bytes.toBytes(mydata._c7))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F8"), Bytes.toBytes(mydata._c8))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F9"), Bytes.toBytes(mydata._c9))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F10"), Bytes.toBytes(mydata._c10))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F11"), Bytes.toBytes(mydata._c11))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F12"), Bytes.toBytes(mydata._c12))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F13"), Bytes.toBytes(mydata._c13))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F14"), Bytes.toBytes(mydata._c14))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F15"), Bytes.toBytes(mydata._c15))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F16"), Bytes.toBytes(mydata._c16))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F17"), Bytes.toBytes(mydata._c17))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F18"), Bytes.toBytes(mydata._c18))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F19"), Bytes.toBytes(mydata._c19))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F20"), Bytes.toBytes(mydata._c20))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F21"), Bytes.toBytes(mydata._c21))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F22"), Bytes.toBytes(mydata._c22))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F23"), Bytes.toBytes(mydata._c23))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F24"), Bytes.toBytes(mydata._c24))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F25"), Bytes.toBytes(mydata._c25))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F26"), Bytes.toBytes(mydata._c26))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F27"), Bytes.toBytes(mydata._c27))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F28"), Bytes.toBytes(mydata._c28))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F29"), Bytes.toBytes(mydata._c29))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F30"), Bytes.toBytes(mydata._c30))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F31"), Bytes.toBytes(mydata._c31))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F32"), Bytes.toBytes(mydata._c32))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F33"), Bytes.toBytes(mydata._c33))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F34"), Bytes.toBytes(mydata._c34))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F35"), Bytes.toBytes(mydata._c35))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F36"), Bytes.toBytes(mydata._c36))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F37"), Bytes.toBytes(mydata._c37))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F38"), Bytes.toBytes(mydata._c38))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F39"), Bytes.toBytes(mydata._c39))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F40"), Bytes.toBytes(mydata._c40))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F41"), Bytes.toBytes(mydata._c41))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F42"), Bytes.toBytes(mydata._c42))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F43"), Bytes.toBytes(mydata._c43))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F44"), Bytes.toBytes(mydata._c44))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F45"), Bytes.toBytes(mydata._c45))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F46"), Bytes.toBytes(mydata._c46))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F47"), Bytes.toBytes(mydata._c47))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F48"), Bytes.toBytes(mydata._c48))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F49"), Bytes.toBytes(mydata._c49))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F50"), Bytes.toBytes(mydata._c50))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F51"), Bytes.toBytes(mydata._c51))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F52"), Bytes.toBytes(mydata._c52))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F53"), Bytes.toBytes(mydata._c53))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F54"), Bytes.toBytes(mydata._c54))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F55"), Bytes.toBytes(mydata._c55))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F56"), Bytes.toBytes(mydata._c56))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F57"), Bytes.toBytes(mydata._c57))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F58"), Bytes.toBytes(mydata._c58))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F59"), Bytes.toBytes(mydata._c59))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F60"), Bytes.toBytes(mydata._c60))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F61"), Bytes.toBytes(mydata._c61))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F62"), Bytes.toBytes(mydata._c62))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F63"), Bytes.toBytes(mydata._c63))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F64"), Bytes.toBytes(mydata._c64))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F65"), Bytes.toBytes(mydata._c65))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F66"), Bytes.toBytes(mydata._c66))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F67"), Bytes.toBytes(mydata._c67))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F68"), Bytes.toBytes(mydata._c68))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F69"), Bytes.toBytes(mydata._c69))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F70"), Bytes.toBytes(mydata._c70))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F71"), Bytes.toBytes(mydata._c71))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F72"), Bytes.toBytes(mydata._c72))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F73"), Bytes.toBytes(mydata._c73))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F74"), Bytes.toBytes(mydata._c74))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F75"), Bytes.toBytes(mydata._c75))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F76"), Bytes.toBytes(mydata._c76))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F77"), Bytes.toBytes(mydata._c77))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F78"), Bytes.toBytes(mydata._c78))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F79"), Bytes.toBytes(mydata._c79))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F80"), Bytes.toBytes(mydata._c80))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F81"), Bytes.toBytes(mydata._c81))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F82"), Bytes.toBytes(mydata._c82))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F83"), Bytes.toBytes(mydata._c83))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F84"), Bytes.toBytes(mydata._c84))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F85"), Bytes.toBytes(mydata._c85))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F86"), Bytes.toBytes(mydata._c86))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F87"), Bytes.toBytes(mydata._c87))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F88"), Bytes.toBytes(mydata._c88))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F89"), Bytes.toBytes(mydata._c89))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F90"), Bytes.toBytes(mydata._c90))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F91"), Bytes.toBytes(mydata._c91))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F92"), Bytes.toBytes(mydata._c92))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F93"), Bytes.toBytes(mydata._c93))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F94"), Bytes.toBytes(mydata._c94))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("F95"), Bytes.toBytes(mydata._c95))

          //PutList += (p)
          
          mutator.mutate(p)
          
          
          /*putcount = putcount + 1
          println("putcount ",putcount)
          if (putcount > 5000) {
            for (ind_put <- PutList) {
              table.put(ind_put)
            }
            putcount = 0
            PutList = new ArrayBuffer[Put]
          }*/
        }
       /* for (ind_put <- PutList) {
              table.put(ind_put)
            }*/
        connection.close()
        //table.close()

      }
    }
    spark.stop
  }
}
