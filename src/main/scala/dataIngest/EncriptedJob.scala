package dataIngest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

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
      .csv("input/")
    println("Lectura del archivo CSV a un Dataframe")
    dfSecurity.show(false)
    println("La cantidad de registros del df es: " + dfSecurity.count())
    dfSecurity.printSchema()

    /*
    El primer paso es concatenar ID_NUMER y ID_TYPE
    */
    var dfOriginalData = dfSecurity.withColumn("IDTYPENUMBER_ORIGINAL",
      concat(dfSecurity.col("ID_NUMBER_ORI"), dfSecurity.col("ID_TYPE")))
    println("Imprimimos el nuevo dataframe con los campos concatenados")
    dfOriginalData.show(false)

    /*
    Encriptamos el campo que tiene concatenado el IDNUMBER Y IDTYPE
    */
    dfOriginalData = dfOriginalData.withColumn("HASH_VALUE_ORI",
      sha2(col("IDTYPENUMBER_ORIGINAL"), 256))
        .drop("ID_NUMBER_ORI", "ID_TYPE")
    println("Imprimimos el nuevo dataframe con Encriptado")
    dfOriginalData.show(false)

    /*
    Ahora tomamos el campo ORIGINAL que tiene concatenado el ID_NUMBER y ID_TYPE y lo dividimos
    */
    dfOriginalData = dfOriginalData
      .withColumn("ID_NUMBER", regexp_extract(col("IDTYPENUMBER_ORIGINAL"),"([0-9]+)", 0))
      .withColumn("ID_TYPE", regexp_extract(col("IDTYPENUMBER_ORIGINAL"), "([a-zA-Z]+)", 0))
      .drop("IDTYPENUMBER_ORIGINAL")
    println("Imprimimos el nuevo dataframe con los campos separados ID_TYPE y ID_NUMBER")
    dfOriginalData.show(false)

    /*
    Ahora procedemos a limpiar el campo de los 0's a la Izquierda
    */
    dfOriginalData = dfOriginalData
      .withColumn("ID_NUMBER_CLEAN", ltrim(col("ID_NUMBER"), "0"))
    println("Imprimimos el nuevo dataframe con el campo ID_NUMBER  limpio de 0's")
    dfOriginalData.show(false)

    /*
    Ahora que esta el campo limpio de 0's procedemos a concatenarlo y encriptarlo nuevamente
    */
    dfOriginalData = dfOriginalData.withColumn("HASH_VALUE_FULL",
      sha2(concat(col("ID_NUMBER_CLEAN"), col("ID_TYPE")), 256))
        .drop("ID_NUMBER", "ID_TYPE")
        .dropDuplicates()
    println("Imprimimos el dataframe final con el campo limpio y encriptado")
    dfOriginalData.show(false)
    println("La cantidad de registros del df es: " + dfOriginalData.count())

  }

}
