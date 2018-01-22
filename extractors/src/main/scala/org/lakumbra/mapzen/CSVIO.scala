package org.lakumbra.mapzen

import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType

trait CSVIO {
  def loadCSVDataFrame(csvFile: String, schemaOpt: Option[StructType]=None,
                       tempView: Option[String]=None,
                       header: Boolean=true,
                       options: Map[String, String]=Map())
                      (implicit sqlContext: SQLContext): DataFrame

  def writeDataFrame(csfFile: String, df: DataFrame)(implicit sqlContext: SQLContext): Unit

}

class SparkCSVIO extends CSVIO{


  override def loadCSVDataFrame(csvFile: String,
                                schemaOpt: Option[StructType]=None,
                                tempView: Option[String]=None,
                                header: Boolean=true,
                                options: Map[String, String]=Map()
                               )(implicit sqlContext: SQLContext): DataFrame = {
    val df = schemaOpt match {
      case Some(schema) =>
        sqlContext.read
          .format("com.databricks.spark.csv")
          .schema(schema)
          .options(options)
          .option("header", "true")
          .option("inferSchema", "false")
          .option("comment", "#")
          .load(csvFile)

      case None =>
        sqlContext.read
          .format("com.databricks.spark.csv")
          .option("header", "true")
          .option("inferSchema", "true")

          .option("comment", "#")
          .load(csvFile)
    }
    for {
      viewName <-tempView
    } yield df.createOrReplaceTempView(viewName)
    df
  }

  override def writeDataFrame(path: String, df: DataFrame)(implicit sqlContext: SQLContext): Unit = {
    df.write
      .mode(SaveMode.Append)
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("quoteAll", "true")
      .option("comment", "#")
      .save(path)
  }

}
