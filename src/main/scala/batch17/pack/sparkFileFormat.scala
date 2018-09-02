package batch17.pack



import batch17.pack.DataFrame_Tutorial.sparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types._


object sparkFileFormat extends App with SparkConfParam {
  case class Person(firstName: String, lastName: String, age:Int)
  import sparkSession.implicits._

  val personRDD = sparkSession.read.textFile("file:///e:/testRun/input/textParquetInput.txt").map(_.split("\t")).map(p => Person(p(0),p(1),p(2).toInt))


 val person = personRDD.toDF()
  person.createOrReplaceTempView("person")
  val sixtyPlus = sparkSession.sql("select * from person where age > 60")
  sixtyPlus.collect.foreach(println)

  sixtyPlus.write.parquet("file:///e:/testRun/output/ParquetOutput")
  sixtyPlus.write.csv("file:///e:/testRun/output/CSVOutput")
  val parquetDF = sparkSession.read.parquet("file:///e:/testRun/output/ParquetOutput")

  parquetDF
    .createOrReplaceTempView("sixty_plus")
  val sixtyPlusNew = sparkSession.sql("select * from sixty_plus").show()

  val parquetDFNew = sparkSession.read.format("file:///e:/testRun/output/ParquetOutput")

  val sqlDF = sparkSession.sql("SELECT * FROM parquet.`file:///e:/testRun/output/ParquetOutput`")
  sqlDF.show()
  sqlDF.printSchema()

  val parquetDFLoad = sparkSession.read.load("file:///e:/testRun/output/ParquetOutput")
  parquetDFLoad.show()
  parquetDFLoad.printSchema()
}