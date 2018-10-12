package org.lakumbra.mapzen

import org.apache.spark.sql.{Dataset, SparkSession, SQLContext, Row}

object PlaceIndexer extends BaseIndexer{

  //scalastyle:off
  def main(args: Array[String]):Unit = {
    implicit val (sparkSession, sqlContext, csvio) = super.init()
    indexCountries(sqlContext)
    indexRegions(sqlContext)
    indexLocalities(sqlContext)
    indexNeighbourhoods(sqlContext)
    indexBoroughs(sqlContext)

  }

  def indexBoroughs(sqlContext: SQLContext)(implicit sparkSession: SparkSession, csvio: CSVIO): Unit = {
    import sparkSession.implicits._
    sqlContext.sql(
      """
        |SELECT
        | bou.id,
        | bou.name,
        | bou.geom_latitude,
        | bou.geom_longitude,
        | bou.lbl_latitude,
        | bou.lbl_longitude,
        | bou.locality_id,
        | bou.superseded_by,
        | bou.cessation,
        | bou.deprecated,
        | loc.name as locality_name,
        | cou.id as country_id,
        | cou.name as country_name,
        | cou.iso_country,
        | loc.region_id as reg_id,
        | reg.name as reg_name,
        | pr.population_rank
        |FROM
        | boroughs bou
        |JOIN locations loc ON bou.locality_id=loc.id
        |LEFT JOIN regions reg ON loc.region_id=reg.id
        |LEFT JOIN population_rank pr ON bou.id=pr.id
        |JOIN countries cou ON loc.country_id=cou.id
      """.stripMargin)
      .repartition(10)
      .filter(r =>
        { r.getAs[String]("superseded_by") == null }
      ).filter(r =>
        { r.getAs[String]("cessation") == "uuuu" || r.getAs[String]("cessation") != "u" }
      ).filter(r =>
        { r.getAs[String]("deprecated") == "uuuu" || r.getAs[String]("cessation") != "u" }
      )
      .map(r => {
        val boroughName = r.getAs[String]("name")
        val localityName = r.getAs[String]("locality_name")
        val countryName = r.getAs[String]("country_name")
        val regionName = r.getAs[String]("reg_name")
        val boroghLocalityName = Seq(boroughName, localityName).filter(_ != null).mkString(" ")
        Place(
          r.getAs[Int]("id"),
          boroughName,
          buildLocation(r),
          r.getAs[Int]("locality_id"),
          "borough",
          Seq(boroghLocalityName, regionName, countryName).filter(_ != null).mkString(","),
          r.getAs[String]("country_name"),
          r.getAs[String]("iso_country"),
          r.getAs[String]("reg_name"),
          localityName,
          boroughName,
          r.getAs[Int]("population_rank")
        )
      }).save("mapzen_places/place", "borough")
  }

  def indexNeighbourhoods(sqlContext: SQLContext)(implicit sparkSession: SparkSession, csvio: CSVIO): Unit = {
    import sparkSession.implicits._
    sqlContext.sql(
      """
        |SELECT
        | nei.id,
        | nei.name,
        | nei.geom_latitude,
        | nei.geom_longitude,
        | nei.lbl_latitude,
        | nei.lbl_longitude,
        | nei.locality_id,
        | nei.superseded_by,
        | nei.cessation,
        | nei.deprecated,
        | loc.name as locality_name,
        | cou.id as country_id,
        | cou.name as country_name,
        | cou.iso_country,
        | loc.region_id as reg_id,
        | reg.name as reg_name,
        | pr.population_rank
        |FROM
        | neighbourhoods nei
        |JOIN locations loc ON nei.locality_id=loc.id
        |LEFT JOIN population_rank pr ON nei.id=pr.id
        |LEFT JOIN regions reg ON loc.region_id=reg.id
        |JOIN countries cou ON loc.country_id=cou.id
      """.stripMargin)
      .repartition(10)
      .filter(r =>
        { r.getAs[String]("superseded_by") == null }
      ).filter(r =>
        { r.getAs[String]("cessation") == "uuuu" || r.getAs[String]("cessation") != "u" }
      ).filter(r =>
        { r.getAs[String]("deprecated") == "uuuu" || r.getAs[String]("cessation") != "u" }
      ).map(r => {
        val neighbourhoodName = r.getAs[String]("name")
        val localityName = r.getAs[String]("locality_name")
        val countryName = r.getAs[String]("country_name")
        val regionName = r.getAs[String]("reg_name")
        val neighbourhoodLocalityName = Seq(regionName, localityName).filter(_ != null).mkString(" ")
        Place(
          r.getAs[Int]("id"),
          neighbourhoodName,
          buildLocation(r),
          r.getAs[Int]("locality_id"),
          "neighbourhood",
          Seq(neighbourhoodLocalityName, regionName, countryName).filter(_ != null).mkString(","),
          r.getAs[String]("country_name"),
          r.getAs[String]("iso_country"),
          r.getAs[String]("reg_name"),
          localityName,
          neighbourhoodName,
          r.getAs[Int]("population_rank")
        )
      }).save("mapzen_places/place", "neighbourhood")
  }

  def indexLocalities(sqlContext: SQLContext)(implicit sparkSession: SparkSession, csvio: CSVIO): Unit = {
    import sparkSession.implicits._
    sqlContext.sql(
      """
        |SELECT
        | loc.id,
        | loc.name,
        | loc.geom_latitude,
        | loc.geom_longitude,
        | loc.lbl_latitude,
        | loc.lbl_longitude,
        | loc.superseded_by,
        | loc.cessation,
        | loc.deprecated,
        | cou.id as country_id,
        | cou.name as country_name,
        | cou.iso_country,
        | loc.region_id as reg_id,
        | reg.name as reg_name,
        | pr.population_rank
        |FROM
        | locations loc
        |LEFT JOIN regions reg ON loc.region_id=reg.id
        |LEFT JOIN population_rank pr ON loc.id=pr.id
        |JOIN countries cou ON loc.country_id=cou.id
      """.stripMargin)
      .repartition(10)
      .filter(r =>
        { r.getAs[String]("superseded_by") == null }
      ).filter(r =>
        { r.getAs[String]("cessation") == "uuuu" || r.getAs[String]("cessation") != "u" }
      ).filter(r =>
        { r.getAs[String]("deprecated") == "uuuu" || r.getAs[String]("cessation") != "u" }
      ).map(r => {
        val localityName = r.getAs[String]("name")
        val countryName = r.getAs[String]("country_name")
        val regionName = r.getAs[String]("reg_name")
        Place(
          r.getAs[Int]("id"),
          r.getAs[String]("name"),
          buildLocation(r),
          r.getAs[Int]("country_id"),
          "locality",
          Seq(localityName, regionName, countryName).filter(_ != null).mkString(","),
          countryName,
          r.getAs[String]("iso_country"),
          regionName,
          localityName,
          "",
          r.getAs[Int]("population_rank")
        )
      }).save("mapzen_places/place", "locality")
  }

  def indexRegions(sqlContext: SQLContext)(implicit sparkSession: SparkSession, csvio: CSVIO): Unit = {
    import sparkSession.implicits._
    sqlContext.sql(
      """
        |SELECT
        | r.id,
        | r.name,
        | r.geom_latitude,
        | r.geom_longitude,
        | r.lbl_latitude,
        | r.lbl_longitude,
        | r.country_id,
        | r.superseded_by,
        | r.cessation,
        | r.deprecated,
        | c.name as country_name,
        | c.iso_country,
        | pr.population_rank
        |FROM regions r
        |LEFT JOIN population_rank pr ON r.id=pr.id
        |JOIN countries c ON r.country_id=c.id
      """.stripMargin
    ).filter(r =>
        { r.getAs[String]("superseded_by") == null }
    ).filter(r =>
        { r.getAs[String]("cessation") == "uuuu" || r.getAs[String]("cessation") != "u" }
    ).filter(r =>
        { r.getAs[String]("deprecated") == "uuuu" || r.getAs[String]("cessation") != "u" }
    ).map(r => {
      val countryName = r.getAs[String]("country_name")
      val regionName = r.getAs[String]("name")
      Place(
        r.getAs[Int]("id"),
        regionName,
        buildLocation(r),
        r.getAs[Int]("country_id"),
        "region",
        Seq(regionName, countryName).filter(_ != null).mkString(","),
        countryName,
        r.getAs[String]("iso_country"),
        regionName,
        "",
        "",
        r.getAs[Int]("population_rank")
      )
    }).save("mapzen_places/place", "region")

  }

  def indexCountries(sqlContext: SQLContext)(implicit sparkSession: SparkSession, csvio: CSVIO): Unit = {
    import sparkSession.implicits._
    sqlContext.sql(
      """
        |SELECT
        | c.id,
        | c.name,
        | c.geom_latitude,
        | c.geom_longitude,
        | c.lbl_latitude,
        | c.lbl_longitude,
        | c.parent_id,
        | c.iso_country,
        | c.superseded_by,
        | c.cessation,
        | c.deprecated,
        | pr.population_rank
        |FROM countries c
        |LEFT JOIN population_rank pr ON c.id=pr.id
      """.stripMargin
    ).filter(r =>
        { r.getAs[String]("superseded_by") == null }
    ).filter(r =>
        { r.getAs[String]("cessation") == "uuuu" || r.getAs[String]("cessation") != "u" }
    ).filter(r =>
        { r.getAs[String]("deprecated") == "uuuu" || r.getAs[String]("cessation") != "u" }
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
        "",
        r.getAs[Int]("population_rank")
      )
    }).save("mapzen_places/place", "country")
  }

  def buildLocation(r: Row): Location={
    val geom_lat = r.getAs[Any]("geom_latitude").toString
    val geom_lng = r.getAs[Any]("geom_longitude").toString
    val lbl_lat = r.getAs[Any]("lbl_latitude").toString
    val lbl_lng = r.getAs[Any]("lbl_longitude").toString

    try {
      if (!lbl_lat.isEmpty && !lbl_lng.isEmpty)
        Location(lbl_lat.toDouble, lbl_lng.toDouble)
      else if (!geom_lat.isEmpty && !geom_lng.isEmpty)
        Location(geom_lat.toDouble, geom_lng.toDouble)
      else
        Location(0.0, 0.0)
    }catch {
      case e:Exception =>
        println(s"geom_lat: $geom_lat geom_lng $geom_lng   lbl_lat: $lbl_lat lbl_lng $lbl_lng")
        Location(0.0, 0.0)
    }
  }

  case class Place(id:Int, name: String, lat_lon: Location, parent_id: Int,  layer: String, label: String, country: String, country_a: String, region: String, locality: String, neighbourhood_borough: String, population_rank:Int)


}
