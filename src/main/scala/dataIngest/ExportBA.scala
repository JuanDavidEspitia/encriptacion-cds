package dataIngest

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
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
    // Creamos la variable de tiempo, para calcular cuando se demora todo el procesp
    val startTimeMillis = System.currentTimeMillis()

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
    def getFileName(): String = {
      val format = new SimpleDateFormat("YYYY-MM-dd_HHmmss")
      var fecha = format.format(Calendar.getInstance().getTime)
      fecha.replace("-", "")
    }
    // Capturamos la fecha en una variable
    val fecha = getFileName()
    println("Fecha: ", fecha)

    /**
     * Metodo para guardar en formato parquet
     * */
    def saveAsParquet(df: DataFrame, fecha: String): Unit =
    {
      try {
        if (!df.head(1).isEmpty) {
          df.repartition(1).write
            .option("delimiter", "|")
            .mode("overwrite")
            .parquet(s"output/parquet/table_security_${fecha}/")
          println("OK  -->  Se guardo Satisfactoriamente en formato Parquet")
        } else {
          println("La cantidad de registros son: " + df.count())
          println("Los parametros estan vacios")
        }
      }catch {
        case e:Exception =>
          println(e)
      }
    }


    /**
     * Metodo para guardar un dataframe en formato CSV
     * */
    def saveAsCSV(df: DataFrame, fecha: String): Unit =
      {
        try {
          if (!df.head(1).isEmpty) {
              df.repartition(1).write
                .format("com.databricks.spark.csv")
                .option("charset", "UTF-8")
                .option("header", "true")
                .mode("overwrite")
                //.option("codec", "gzip")   -- para comprimir el archivo en formato gzip
                .save(s"output/csv/table_security_$fecha/")
          } else {
            println("La cantidad de registros son: " + df.count())
            println("Los parametros estan vacios")
          }
        }catch {
          case e:Exception =>
            println(e)
        }
      }


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

    //Creamos un Dataframe Vacio
    var dfEmpty = spark.emptyDataFrame

    /**
     * Llamamos al metodo de guardar en formato Parquet
     * */
      saveAsParquet(dfSecurity, fecha)

    /**
     * Llamamos al metodo de guardar en formato CSV
     * */
      saveAsCSV(dfSecurity, fecha)


    /**
     * Capturamos las columnas en un Dataframe o Array
     * */
    dfSecurity.printSchema()
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


    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
    println("El tiempo empleado en Segundos es: " + durationSeconds +"sg")

  }

}
