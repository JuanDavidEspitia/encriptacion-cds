package dataIngest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{concat, sha2, trim}

object ExportBA
{
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("spark").setLevel(Level.WARN)
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Caso de Uso Export BA")
      .getOrCreate()

    /**
     * Cargamos el Archivo CSV en un Dataframe ->  dfSecurity
     * */

    val path_csv = "input/table_security.csv"
    val delimiter = ";"

    var dfSecurity = spark.read
      .option("header", "true")
      .option("mergeSchema", "true")
      .option("inferSchema", "true")
      .option("delimiter", delimiter)
      .csv(path_csv)
    println("---------------        IMPRIMIMOS EL DATAFRAME CON LA DATA CARGADA      ---------------")
    dfSecurity.show(5)

    /**
     * Tomamos los campos de ID y TIPO de cliente para concatenarlos y aplicar Hash 256
     * */
    dfSecurity = dfSecurity.withColumn("HASH_CLIENTE",
      sha2(concat(trim(
        dfSecurity.col("ID_CLIENTE")), dfSecurity.col("TIPO_CLIENTE")), 256))
    println("---------------        IMPRIMIMOS EL DATAFRAME CON LOS CAMPOS DE CLIENTE HASHEADOS      ---------------")
    dfSecurity.show(5)
    /**
     * Tomamos los campos de ID y TIPO de Proveedor para concatenarlos y aplicar Hash 256
     * */
    dfSecurity = dfSecurity.withColumn("HASH_PROVEDOR",
      sha2(concat(trim(
        dfSecurity.col("ID_PROVEDOR")), dfSecurity.col("TIPO_PROVEDOR")), 256))
    println("---------------        IMPRIMIMOS EL DATAFRAME CON LOS CAMPOS DE PROVEEDOR HASHEADOS      ---------------")
    dfSecurity.show(5)
    /**
     * Tomamos los campos de ID y TIPO de Canal para concatenarlos y aplicar Hash 256
     * */
    dfSecurity = dfSecurity.withColumn("HASH_CANAL",
      sha2(concat(trim(
        dfSecurity.col("ID_CANAL")), dfSecurity.col("ID_CANAL")), 256))
    println("---------------        IMPRIMIMOS EL DATAFRAME CON LOS CAMPOS DE PROVEEDOR HASHEADOS      ---------------")
    dfSecurity.show(5)

    /**
     * Capturamos las columnas en un Dataframe o Array
     * */
    dfSecurity.printSchema()
    //Creamos un Dataframe Vacio
    var df = spark.emptyDataFrame
    //Creamos una lista vacia
    var dm  = List[String]()

    val selectColumns = dfSecurity.columns.toSeq
    val selectColumns2 = dfSecurity.columns.toList
    println(selectColumns)
    println("Conversion a Lista")
    println(selectColumns2)
    /**
     * Convertimos de Seq a Dataframe
     * */












  }

}
