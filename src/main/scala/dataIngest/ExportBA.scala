package dataIngest

import java.text.SimpleDateFormat
import java.util.Calendar

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
     * Funcion que obtiene la fecha para nombrar el archivo de salida
     * */
    def getDataName(): String = {
      val format = new SimpleDateFormat("YYYY-MM-dd_HHmmss")
      var fecha = format.format(Calendar.getInstance().getTime)
      fecha.replace("-", "")
    }
    // Capturamos la fecha en una variable
    val fecha = getDataName()
    println("Fecha: ", fecha)


    /**
     * Tomamos los campos de ID y TIPO de cliente para concatenarlos y aplicar Hash 256
     * */
    dfSecurity = dfSecurity.withColumn("HASH_1",
      sha2(concat(trim(
        dfSecurity.col("ID_CLIENTE")), dfSecurity.col("TIPO_CLIENTE")), 256))
    println("---------------        IMPRIMIMOS EL DATAFRAME CON LOS CAMPOS DE CLIENTE HASHEADOS      ---------------")
    dfSecurity.show(5)
    /**
     * Tomamos los campos de ID y TIPO de Proveedor para concatenarlos y aplicar Hash 256
     * */
    dfSecurity = dfSecurity.withColumn("HASH_2",
      sha2(concat(trim(
        dfSecurity.col("ID_PROVEDOR")), dfSecurity.col("TIPO_PROVEDOR")), 256))
    println("---------------        IMPRIMIMOS EL DATAFRAME CON LOS CAMPOS DE PROVEEDOR HASHEADOS      ---------------")
    dfSecurity.show(5)
    /**
     * Tomamos los campos de ID y TIPO de Canal para concatenarlos y aplicar Hash 256
     * */
    dfSecurity = dfSecurity.withColumn("HASH_3",
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

    println("Sacamos las colmnas en una Seq -> Conversion a Seq")
    val SeqColumns = dfSecurity.columns.toSeq


    /**
     * Metodo 1 --> Obetener las colmunas que contengan la palabra hash y guardarlos en una lista
     * */
    println("Obtenemos las columnas en una Lista -> Conversion a Lista")
    //var ListColumns = dfSecurity.columns.toList.map(_.toLowerCase()) // Pasamos a lista y a minuscula
    //var listHash = dfSecurity.columns.toList.filter(x => Seq("hash").exists(y => x.toLowerCase.contains(y.toLowerCase())))
    var listHash = dfSecurity.columns.toList
      .filter(x => Seq("hash").exists(y => x.toLowerCase.contains(y.toLowerCase())))
    listHash = listHash.map(_.toLowerCase())

    for (n <- listHash )
      {
        println("Elemento del for: " + n)
      }



    /**
     * Convertimos la lista a Dataframe
     * */
    import spark.implicits._
    var dfColumns = listHash.toDF()

    dfColumns.show(false)
    /**
     * Convertimos la lista a RDD
     * */
    val rdd = spark.sparkContext.parallelize(listHash)

    //dfColumns.select(dfColumns.col("value"), regexp_extract())
    val singleReg = """HASH_*""".r
    //val validSingleRecords = rdd.filter()

    /**
     * Filtramos los registros que cumplan con la condicion de HASH --> METODO 2
     * */
      println("Creamos un Dataframe con las columnas filtradas por hash")
    val dffilter = dfColumns.filter($"value" rlike "hash_*")
    dffilter.show(false)
    println(dffilter.count())














  }

}
