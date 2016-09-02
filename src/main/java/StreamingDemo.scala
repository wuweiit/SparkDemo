/**
  * Created by Administrator on 2016/9/1.
  */

/* SimpleApp.scala */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3

object StreamingDemo {
  def main(args: Array[String]) {
//    val logFile = "README.md" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application")
    val ssc = new StreamingContext(conf, Seconds(5))

    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("localhost", 9999)

    // Split each line into words
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }
}