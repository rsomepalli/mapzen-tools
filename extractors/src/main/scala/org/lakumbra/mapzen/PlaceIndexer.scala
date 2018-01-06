package org.lakumbra.mapzen

object PlaceIndexer extends BaseIndexer{

  //scalastyle:off
  def main(args: Array[String]):Unit = {

    implicit val (sparkSession, sqlContext) = super.init()
    import sparkSession.implicits._

    sqlContext.sql(
      """
        |select r.id, r.name, r.geom_latitude, r.geom_longitude, r.country_id, c.name as country_name, c.iso_country
        |from regions r
        |join countries c on r.country_id=c.id
      """.stripMargin
    ).map(r =>{
      val countryName = r.getAs[String]("country_name")
      val regionName = r.getAs[String]("name")
      Place(
      r.getAs[Int]("id"),
      regionName,
      Location(r.getAs[Double]("geom_latitude"), r.getAs[Double]("geom_longitude")),
      r.getAs[Int]("country_id"),
      "region",
      s"$regionName, $countryName ",
      r.getAs[String]("country_name"),
      r.getAs[String]("iso_country"),
      regionName,
      ""
    )
    }).save("mapzen_places/place")

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
      .map(r =>{
        val localityName = r.getAs[String]("name")
        val countryName = r.getAs[String]("country_name")
        val regionName = r.getAs[String]("reg_name")
        Place(
          r.getAs[Int]("id"),
          r.getAs[String]("name"),
          Location(r.getAs[Double]("geom_latitude"), r.getAs[Double]("geom_longitude")),
          r.getAs[Int]("country_id"),
          "locality",
          s"$localityName, $regionName, $countryName ",
          r.getAs[String]("country_name"),
          r.getAs[String]("iso_country"),
          r.getAs[String]("reg_name"),
          r.getAs[String]("name")
        )
      }).save("mapzen_places/place")

    sqlContext.sql(
      """
        |select id, name, geom_latitude, geom_longitude, parent_id, iso_country from countries
      """.stripMargin
    ).map(r =>{
      val countryName = r.getAs[String]("name")
      Place(
        r.getAs[Int]("id"),
        countryName,
        Location(r.getAs[Double]("geom_latitude"), r.getAs[Double]("geom_longitude")),
        r.getAs[Int]("parent_id"),
        "country",
        countryName,
        countryName,
        r.getAs[String]("iso_country"),
        "",
        ""
      )
    }).save("mapzen_places/place")

  }
  case class Place(id:Int, name: String, lat_lon: Location, parent_id: Int,  layer: String, label: String, country: String, country_a: String, region: String, locality: String)

}

/**
{
  "state": "open",
  "settings": {
    "index": {
      "number_of_shards": "5",
      "number_of_replicas": "1",
      "analysis": {
        "filter": {
          "autocomplete_filter": {
            "type": "edge_ngram",
            "min_gram": 1,
            "max_gram": 20
          }
        },
        "analyzer": {
          "autocomplete": {
            "type": "custom",
            "tokenizer": "standard",
            "filter": [
              "lowercase",
              "autocomplete_filter"
            ]
          }
        }
      }
    }
  },
  "mappings": {
    "place": {
      "properties": {
        "id": {
          "type": "long",
          "index": "not_analyzed"
        },
        "layer": {
          "type": "string",
          "index": "not_analyzed"
        },
        "name": {
          "type": "string"
        },
        "lat_lon": {
          "type": "geo_point"
        },
        "parent_id": {
          "type": "long"
        },
        "country": {
          "type": "string",
          "index": "not_analyzed"
        },
        "country_a": {
          "type": "string",
          "index": "not_analyzed"
        },
        "region": {
          "type": "string",
          "index": "not_analyzed"
        },
        "locality": {
          "type": "string",
          "index": "not_analyzed"
        },
        "label": {
          "type": "string",
          "analyzer": "autocomplete"
        }
      }
    }
  }
}/*