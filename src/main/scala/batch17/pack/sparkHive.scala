package batch17.pack

import
org.apache.spark.sql.{Row, SaveMode, SparkSession}
import java.io.File
import org.apache.spark.sql.hive._

object sparkHive extends App with SparkConfParam {

case class Record(key: Int, value: String)

import sparkSession.implicits._
import sparkSession.sql

  sparkSession.sql("CREATE TABLE IF NOT EXISTS srcnew (firstName STRING, lastName STRING, age INT) USING hive")
  sparkSession.sql("LOAD DATA LOCAL INPATH 'file:///e:/testRun/input/textParquetInput.txt' INTO TABLE srcnew")

// Queries are expressed in HiveQL
  sparkSession.sql("SELECT * FROM srcnew").show()
}


/*

//fetch metadata data from the catalog
    spark.catalog.listDatabases.show(false)
    spark.catalog.listTables.show(false)

 */