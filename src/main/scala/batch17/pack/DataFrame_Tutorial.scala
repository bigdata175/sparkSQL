package batch17.pack

import batch17.pack.DataFrame_Tutorial.sparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types._


object DataFrame_Tutorial extends App with SparkConfParam {

  import sparkSession.implicits._
  val customSchema = StructType(Array(
    StructField("eID", IntegerType, false),
    StructField("eTAG", StringType, true)))
/*
  // Create a DataFrame from reading a CSV file
  val dfTags = sparkSession
    .read.format("csv")
    .option("sep",",")
    .option("header", "true")
   // .schema(customSchema)
    .option("inferSchema", "false")
    .load("file:///e:/testRun/input/question_tags_10K.csv")
    .toDF()

  println(" data from CSV : ")
  dfTags.printSchema()
  dfTags.show()
  // dfTags.write.option("header", "true").csv("file:///e:/testRun/output/DataFrame_Tutorial/text/");


  val dfQuestions = dfTags.select(
    dfTags.col("Id").cast("integer")).filter("Id > 25 and Id < 30")

  dfQuestions.printSchema()
  dfQuestions.show()
  dfQuestions.join(dfTags, "Id").show(10)

  dfTags.createOrReplaceTempView("people")
  sparkSession.catalog.listTables().show()
  sparkSession.sql("show tables").show()

  sparkSession
    .sql(
      """select
        |count(*) as php_count
        |from people where tag='php'""".stripMargin)
    .show(10)

*/


  val jsonDF = sparkSession.read.json("file:///E:/testRun/input/testJ.json")

  

  jsonDF.printSchema()

    jsonDF.show()
  jsonDF.select("name").show()
  jsonDF.filter("age > 21").show()
  jsonDF.groupBy("age").count().show()
  //jsonDF.select("name", "age" + 1).show()

  //jsonDF.createOrReplaceTempView("people")

  jsonDF.createGlobalTempView("people")

  val sqlDF = sparkSession.sql("SELECT name, age+1 FROM global_temp.people")
  sqlDF.show()
  sparkSession.newSession().sql("SELECT * FROM global_temp.people").show()

  case class Person(name: String, age: Long)
  val peopleDS  = sparkSession.read.json("file:///E:/testRun/input/testJ.json").as[Person]
  peopleDS.show()
  peopleDS.select($"name", $"age" + 5).show()

  /*
  val df = sparkSession.read.parquet("file:///e:/testRun/output/DataFrame_Tutorial")
   println(" data from parquet : ")
  df.show()
*/
  //  dfTags.write.format("com.databricks.spark.csv").option("header", "true").save("file:///e:/testRun/output/DataFrame_Tutorial.csv")
  // dfTags.write.parquet("file:///e:/testRun/output/DataFrame_Tutorial");

  //  dfTags.write.format("com.databricks.spark.avro").save("file:///e:/testRun/output/DataFrame_Tutorial")
  // dfTags.write.orc("file:///e:/testRun/output/DataFrame_Tutorial")




/*
.schema(customSchema)
val customSchema = StructType(Array(
    StructField("ID", IntegerType, true),
    StructField("TAG", StringType, true)))

*/



}