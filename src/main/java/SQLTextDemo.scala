/**
  * Created by Administrator on 2016/9/1.
  */

/* SimpleApp.scala */
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.sql.SparkSession


object SQLTextDemo {
  def main(args: Array[String]) {
//    val logFile = "README.md" // Should be some file on your system
//    val conf = new SparkConf().setAppName("Simple Application")
//    val sc = new SparkContext(conf)

    val spark =  SparkSession.builder().appName("sql test")
      .config("spark.sql.warehouse.dir", "file:///C:/spark-warehouse")
      .getOrCreate();

    case class Person(name: String, age: Long)



    val rdd = spark.sparkContext
      .textFile("examples/src/main/resources/people.txt")
      .map(_.split(","))
      .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
    .toJavaRDD();
    // RDD -> df
    val df =  spark.createDataFrame(rdd,null);

    // Displays the content of the DataFrame to stdout
    df.show()

    df.createOrReplaceTempView("people")

    val sqlDF = spark.sql("SELECT * FROM people where age>19")
    sqlDF.show()

  }
}