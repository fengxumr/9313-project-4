package comp9313.ass4

import org.apache.spark._
import scala.collection.mutable.ArrayBuffer

object SetSimJoin {

  // input: records having the same prefix; output: matching pairs in format -- arraybuffer -- (ID1, ID2, Similarity)
  def filterPairs(pairs: Tuple2[String, Iterable[(Int, Int, Int, Array[String])]], t: Double) : ArrayBuffer[Tuple3[Int, Int, Double]] = {
    val buf = ArrayBuffer[Tuple3[Int, Int, Double]]()
    val data = pairs._2.toArray
    val len = data.size
    if (len >= 2) {
      for (i <- 0 until len) {
        for (j <- i+1 until len) {
          if (data(j)._2 >= data(i)._3) {
            val com = data(i)._4.toSet.&(data(j)._4.toSet).size.toDouble
            val sim = com / (data(i)._2 + data(j)._2 - com)
            if (sim >= t) {
              buf.append((data(i)._1, data(j)._1, sim))
            }
          }
        }
      }
    }
    buf
  }

  def main(args: Array[String])= {
    val inputFile = args(0)
    val outputFolder = args(1)
    val t = args(2).toDouble
    val conf = new SparkConf().setAppName("SetSimJoin")
    val sc = new SparkContext(conf)
    val input = sc.textFile(inputFile).map(_.split(" "))
    input.persist()

    // wordMap is a map that map the words in contents to their quantities in the whole file.
    val wordMap = input.flatMap(x => x.map(y => y)).map(x => (x, 1)).reduceByKey(_+_).sortBy(_._2).collectAsMap()
    // broadcast wordMap
    val broad = sc.broadcast(wordMap)

    // data is RDD which elements are arrays -- array(ID, content length, length filter value, content, prefix content)
    val data = input.map(x => (x(0), x.size-1, Math.ceil((x.size-1)*t).toInt, x.drop(1).sortBy(x => (broad.value(x), x)), (x.size-1)-Math.ceil((x.size-1)*t).toInt+1)).map(x => (x._1.toInt, x._2, x._3, x._4, x._4.dropRight(x._2-x._5)))

    // dataOutput is RDD that contains all output array with format -- (ID1, ID2, Similarity)
    val dataOutput = data.flatMap(x => x._5.map(y => (y, (x._1, x._2, x._3, x._4)))).groupByKey().flatMap(x => filterPairs(x, t)).distinct

    // sorting and format conversion before saving as textfile
    dataOutput.sortBy(x => (x._1, x._2)).map(x => ((x._1,x._2) + "\t" + x._3)).saveAsTextFile(outputFolder)

  }
}
