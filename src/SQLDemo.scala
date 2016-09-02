/**
  * Created by Administrator on 2016/9/1.
  */

/* SimpleApp.scala */
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}


object SQLDemo {
  def main(args: Array[String]) {
//    val logFile = "README.md" // Should be some file on your system
//    val conf = new SparkConf().setAppName("Simple Application")
//    val sc = new SparkContext(conf)

    val spark =  SparkSession.builder().appName("sql test")
      .config("spark.sql.warehouse.dir", "file:///C:/spark-warehouse")
      .getOrCreate();


    val df = spark.read.json("examples/src/main/resources/people.json")

    // Displays the content of the DataFrame to stdout
    df.show()

    df.createOrReplaceTempView("people")

    val sqlDF = spark.sql("SELECT * FROM people where age>19")
    sqlDF.show()

  }
}