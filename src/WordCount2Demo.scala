/**
  * Created by Administrator on 2016/9/1.
  */

/* SimpleApp.scala */
import org.apache.spark.{SparkConf, SparkContext}

object WordCount2Demo {
  def main(args: Array[String]) {
    val logFile = "README.md" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)





    val logData = sc.textFile(logFile, 2).cache()


    val numAs = logData.filter(line => line.contains("Spark")).count()
    println("Lines with Spark: %s ".format(numAs))
  }
}