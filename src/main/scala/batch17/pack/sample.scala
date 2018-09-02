package batch17.pack

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object one {
  def main(args: Array[String]): Unit =
  {
    println("this is object1")
  }
}

object sample {

  def main(args: Array[String]): Unit ={
    lazy val sparkConf = new SparkConf()
      .setAppName("Learn Spark")
      .setMaster("local")
      .set("spark.cores.max", "2")

    lazy val sparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    import sparkSession.implicits._

    val jsonDF = sparkSession.read.json("/user/dl_loader/spark-test/input/sparkcore/testJ.json")

    //val jsonDF = sc.read.json("file:///E:/testRun/input/testJ.json")



    jsonDF.printSchema()

    jsonDF.show()
    jsonDF.select("name").show()
    jsonDF.filter("age > 21").show()
    jsonDF.groupBy("age").count().show()

    jsonDF.createOrReplaceTempView("people")
    val sqlDF = sparkSession.sql("SELECT * FROM people")
    sqlDF.show()













  }

}
