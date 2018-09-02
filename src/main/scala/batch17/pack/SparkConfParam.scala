package batch17.pack

import java.util.Properties
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.io.Source
import java.io.File

trait SparkConfParam {

  val props = new Properties()
  props.load((Source.fromFile("e:/testRun/input/application.properties")).bufferedReader())
  println(props)
  val warehouseLocation = "file:///D:/tmp/hive/srikanth.paruchuri/988a17b8-c5e7-453f-87c6-22cdae68745a/_tmp_space.db"
  lazy val sparkConf = new SparkConf()
    .setAppName("Learn Spark")
    .setMaster(props.getProperty("master.value"))
    .set("spark.cores.max", "2")
    .set("spark.sql.parquet.binaryAsString","true")
  lazy val sparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .enableHiveSupport()
    .getOrCreate()
  import sparkSession.implicits._

}
