package dataIngest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{concat, regexp_extract, sha2, trim}

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
    var list  = List[String]()

    val SeqColumns = dfSecurity.columns.toSeq
    val ListColumns = dfSecurity.columns.toList
    println(SeqColumns)
    println("Conversion a Lista")
    println(ListColumns)
    /**
     * Convertimos la lista a Dataframe
     * */
    import spark.implicits._
    var dfColumns = ListColumns.toDF()
    dfColumns.show(false)
    /**
     * Convertimos la lista a RDD
     * */
    val rdd = spark.sparkContext.parallelize(ListColumns)

    //dfColumns.select(dfColumns.col("value"), regexp_extract())
    val singleReg = """HASH_*""".r
    //val validSingleRecords = rdd.filter()

    /**
     * Filtramos los registros que cumplan con la condicion de HASH
     * */
    val dffilter = dfColumns.filter($"value" rlike "HASH_*")
    dffilter.show(false)
    println(dffilter.count())



  }

}
