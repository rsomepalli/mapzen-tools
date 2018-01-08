package org.lakumbra.mapzen

import org.apache.commons.math3.ml.neuralnet.SquareNeighbourhood
import org.apache.spark.sql.{SparkSession, SQLContext, Row}

object PlaceIndexer extends BaseIndexer{

  //scalastyle:off
  def main(args: Array[String]):Unit = {

    implicit val (sparkSession, sqlContext) = super.init()

    indexCountries(sqlContext)
    indexRegions(sqlContext)
    indexLocalities(sqlContext)
    indexNeighbourhoods(sqlContext)
    indexBoroughs(sqlContext)


  }

  def indexBoroughs(sqlContext: SQLContext)(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    sqlContext.sql(
      """
        |SELECT
        | bou.id,
        | bou.name,
        | bou.geom_latitude,
        | bou.geom_longitude,
        | bou.locality_id,
        | loc.name as locality_name,
        | cou.id as country_id,
        | cou.name as country_name,
        | cou.iso_country,
        | loc.region_id as reg_id,
        | reg.name as reg_name
        |FROM
        | boroughs bou
        | join locations loc on bou.locality_id=loc.id
        | left join regions reg on loc.region_id=reg.id
        | join countries cou on loc.country_id=cou.id
      """.stripMargin)
      .repartition(10)
      .map(r => {
        val boroughName = r.getAs[String]("name")
        val localityName = r.getAs[String]("locality_name")
        val countryName = r.getAs[String]("country_name")
        val regionName = r.getAs[String]("reg_name")
        Place(
          r.getAs[Int]("id"),
          boroughName,
          buildLocation(r),
          r.getAs[Int]("locality_id"),
          "borough",
          s"$boroughName $localityName, $regionName, $countryName",
          r.getAs[String]("country_name"),
          r.getAs[String]("iso_country"),
          r.getAs[String]("reg_name"),
          localityName,
          boroughName
        )
      }).save("mapzen_places/place")
  }

  def indexNeighbourhoods(sqlContext: SQLContext)(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    sqlContext.sql(
      """
        |SELECT
        | nei.id,
        | nei.name,
        | nei.geom_latitude,
        | nei.geom_longitude,
        | nei.locality_id,
        | loc.name as locality_name,
        | cou.id as country_id,
        | cou.name as country_name,
        | cou.iso_country,
        | loc.region_id as reg_id,
        | reg.name as reg_name
        |FROM
        | neighbourhoods nei
        | join locations loc on nei.locality_id=loc.id
        | left join regions reg on loc.region_id=reg.id
        | join countries cou on loc.country_id=cou.id
      """.stripMargin)
      .repartition(10)
      .map(r => {
        val neighbourhoodName = r.getAs[String]("name")
        val localityName = r.getAs[String]("locality_name")
        val countryName = r.getAs[String]("country_name")
        val regionName = r.getAs[String]("reg_name")
        Place(
          r.getAs[Int]("id"),
          neighbourhoodName,
          buildLocation(r),
          r.getAs[Int]("locality_id"),
          "neighbourhood",
          s"$neighbourhoodName $localityName, $regionName, $countryName",
          r.getAs[String]("country_name"),
          r.getAs[String]("iso_country"),
          r.getAs[String]("reg_name"),
          localityName,
          neighbourhoodName
        )
      }).save("mapzen_places/place")
  }

  def indexLocalities(sqlContext: SQLContext)(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    sqlContext.sql(
      """
        |SELECT
        |loc.id,
        | loc.name,
        | loc.geom_latitude,
        | loc.geom_longitude,
        | cou.id as country_id,
        | cou.name as country_name,
        | cou.iso_country,
        | loc.region_id as reg_id,
        | reg.name as reg_name
        |FROM
        | locations loc left join regions reg on loc.region_id=reg.id
        | join countries cou on loc.country_id=cou.id
      """.stripMargin)
      .repartition(10)
      .map(r => {
        val localityName = r.getAs[String]("name")
        val countryName = r.getAs[String]("country_name")
        val regionName = r.getAs[String]("reg_name")
        Place(
          r.getAs[Int]("id"),
          r.getAs[String]("name"),
          buildLocation(r),
          r.getAs[Int]("country_id"),
          "locality",
          s"$localityName, $regionName, $countryName ",
          countryName,
          r.getAs[String]("iso_country"),
          regionName,
          localityName,
          ""
        )
      }).save("mapzen_places/place")
  }

  def indexRegions(sqlContext: SQLContext)(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    sqlContext.sql(
      """
        |select r.id, r.name, r.geom_latitude, r.geom_longitude, r.country_id, c.name as country_name, c.iso_country
        |from regions r
        |join countries c on r.country_id=c.id
      """.stripMargin
    ).map(r => {
      val countryName = r.getAs[String]("country_name")
      val regionName = r.getAs[String]("name")
      Place(
        r.getAs[Int]("id"),
        regionName,
        buildLocation(r),
        r.getAs[Int]("country_id"),
        "region",
        s"$regionName, $countryName ",
        countryName,
        r.getAs[String]("iso_country"),
        regionName,
        "",
        ""
      )
    }).save("mapzen_places/place")
  }

  def indexCountries(sqlContext: SQLContext)(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    sqlContext.sql(
      """
        |select id, name, geom_latitude, geom_longitude, parent_id, iso_country from countries
      """.stripMargin
    ).map(r => {
      val countryName = r.getAs[String]("name")
      Place(
        r.getAs[Int]("id"),
        countryName,
        buildLocation(r),
        r.getAs[Int]("parent_id"),
        "country",
        countryName,
        countryName,
        r.getAs[String]("iso_country"),
        "",
        "",
        ""
      )
    }).save("mapzen_places/place")
  }

  def buildLocation(r: Row): Location={
    val lat = r.getAs[Any]("geom_latitude").toString
    val lng = r.getAs[Any]("geom_longitude").toString
    try {
      if (lat.isEmpty || lng.isEmpty)
        Location(0.0, 0.0)
      else
        Location(lat.toDouble, lng.toDouble)
    }catch {
      case e:Exception =>
        println(s"lat: $lat lng $lng")
        Location(0.0, 0.0)
    }
  }

  case class Place(id:Int, name: String, lat_lon: Location, parent_id: Int,  layer: String, label: String, country: String, country_a: String, region: String, locality: String, neighbourhood_borough: String)

}

