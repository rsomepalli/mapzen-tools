package org.lakumbra.mapzen

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql._

class BaseIndexer {

  def init(): (SparkSession, SQLContext, CSVIO) = {
    val csvio = new SparkCSVIO
    val config = ConfigFactory.load()
    val ss = SparkSession.builder()
      .config("es.index.auto.create", "true")
      .config("es.nodes", config.getString("lakumbra.es.nodes"))
      .config("es.cluster.name" ,config.getString("lakumbra.es.cluster"))
      .appName("MapZenLocationIndexer")
      .master("local")
      .getOrCreate()

    val baseFolder = config.getString("lakumbra.input-files.folder")
    implicit val sqlContext = ss.sqlContext
    csvio.loadCSVDataFrame(s"${baseFolder}/wof-continent-latest.csv")
      .select("id", "name", "geom_latitude", "geom_longitude", "lbl_latitude", "lbl_longitude" )
      .createOrReplaceTempView("continents")

    csvio.loadCSVDataFrame(s"$baseFolder/wof-country-latest.csv")
      .select("id", "name", "geom_latitude", "geom_longitude" , "parent_id", "iso_country", "lbl_latitude", "lbl_longitude")
      .createOrReplaceTempView("countries")

    csvio.loadCSVDataFrame(s"$baseFolder/wof-region-latest.csv")
      .select("id", "name", "geom_latitude", "geom_longitude" , "country_id", "lbl_latitude", "lbl_longitude")
      .createOrReplaceTempView("regions")

    csvio.loadCSVDataFrame(s"$baseFolder/wof-locality-latest.csv")
      .select("id", "name", "geom_latitude", "geom_longitude", "region_id", "country_id" , "lbl_latitude", "lbl_longitude")
      .createOrReplaceTempView("locations")

    csvio.loadCSVDataFrame(s"$baseFolder/wof-neighbourhood-latest.csv")
      .select("id", "name", "geom_latitude", "geom_longitude", "locality_id", "region_id", "country_id" , "lbl_latitude", "lbl_longitude")
      .createOrReplaceTempView("neighbourhoods")

    csvio.loadCSVDataFrame(s"$baseFolder/wof-borough-latest.csv")
      .select("id", "name", "geom_latitude", "geom_longitude", "locality_id", "region_id", "country_id" , "lbl_latitude", "lbl_longitude")
      .createOrReplaceTempView("boroughs")

    csvio.loadCSVDataFrame(s"$baseFolder/wof-population-rank.csv")
      .select("id", "population_rank")
      .createOrReplaceTempView("population_rank")

    (ss, sqlContext, csvio)
  }

  implicit class  ESIndexer(df: Dataset[_]){
    def save(indexName: String):Unit = {
      df.write.mode(SaveMode.Append)
        .format("org.elasticsearch.spark.sql")
        .option("es.resource", indexName)
        .save()
    }
  }


}
case class Location(lat:Double, lon: Double)

