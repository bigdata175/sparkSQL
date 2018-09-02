package batch17.pack


import batch17.pack.DataFrame_Tutorial.sparkSession
import org.apache.spark
import org.apache.spark.sql.types._
import spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

object dfexample{

  def main(args: Array[String]): Unit ={

    val sparkConf = new SparkConf()
      .setAppName("Learn Spark")
      .setMaster("local")
      .set("spark.cores.max", "2")

    val sparkSession = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sparkSession)

    val peopleRDD = sparkSession.textFile("file:///D:\\spark-2.3.1-bin-hadoop2.7\\emp.txt")

    // peopleRDD contains first record as header ex: name age
    val schemaString  = peopleRDD.first().split(",").to[List]
    // Now schemaString contains record as name age
    // val schemaString = "name age"
    // Generate the schema based on the string of schema

    val fields = schemaString.flatMap(x => x.split(" ")).map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)
    // Convert records of the RDD (people) to Rows
    val rowRDD1 = peopleRDD.mapPartitionsWithIndex((index, it) => if(index == 0) it.drop(1) else it)
    val rowRDD = rowRDD1.map(_.split(",")).map(attributes => {Row(attributes(0), attributes(1))})
    rowRDD.foreach(println)
    // Apply the schema to the RDD
    val peopleDF = sqlContext.createDataFrame(rowRDD, schema)
    peopleDF.printSchema()
    peopleDF.show()
    // Creates a temporary view using the DataFrame
    peopleDF.createOrReplaceTempView("people")

    // SQL can be run over a temporary view created using DataFrames
    val results = sqlContext.sql("SELECT name FROM people")
    results.show()
    peopleDF.write.format("parquet").mode(SaveMode.Append).option("path", "file:///D:\\saveAsEmp1").saveAsTable("temp")
    val results1 = sqlContext.sql("SELECT name FROM temp")
    results1.show()

    peopleDF.write.format("json").partitionBy("name").bucketBy(3, "age").option("path", "file:///D:\\partTable").saveAsTable("partitionedTable")

    val results2 = sqlContext.sql("SELECT name FROM partitionedTable")
    results2.show()
  }}

/*

SaveMode.ErrorIfExists
SaveMode.Append
SaveMode.Overwrite
SaveMode.Ignore
SaveMode.Ignore

.saveAsTable("temp")

peopleDF.write.format("json").partitionBy("age").bucketBy(3, "name").option("path", "file:///D:\\partTable").saveAsTable("partitionedTable")
 */