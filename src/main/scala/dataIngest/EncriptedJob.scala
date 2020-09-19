package dataIngest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object EncriptedJob
{
  def main(args: Array[String]): Unit =
  {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("ETL BA - CDS").master("local[*]").getOrCreate()

    // Cargamos los archivos base a un df
    val dfSecurity = spark.read
      .option("header", "true")
      .option("mergeSchema", "true")
      .option("delimiter", ";")
      .csv("input/security-dataprueba-bdb.csv")
    dfSecurity.show(false)






  }

}
