package org.lakumbra.mapzen

import com.typesafe.config.ConfigFactory

object Debugging extends BaseIndexer{


  //scalastyle:off
  def main(args: Array[String]):Unit= {
    implicit val (sparkSession, sqlContext, csvio) = super.init()
    val config = ConfigFactory.load()
    val baseFolder = config.getString("lakumbra.input-files.folder")
    import sparkSession.implicits._
    csvio.loadCSVDataFrame(s"$baseFolder/wof-region-latest.csv").createOrReplaceTempView("reg_raw")

    sqlContext.sql(
      """
        |select r.id, r.name, r.geom_latitude, r.geom_longitude, r.country_id, c.name as country_name, c.iso_country
        |from regions r
        |join countries c on r.country_id=c.id
      """.stripMargin
    ).createOrReplaceTempView("reg_joined")

    sqlContext.sql(
      """
        |select * from reg_raw r  where not exists(select rj.id from reg_joined rj where r.id=rj.id )
      """.stripMargin).show(1000)


    csvio.loadCSVDataFrame(s"$baseFolder/wof-locality-latest.csv").createOrReplaceTempView("loc_raw")

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
      """.stripMargin).createOrReplaceTempView("loc_joined")

    sqlContext.sql(
          """
            |select * from loc_raw l  where not exists(select lj.id from loc_joined lj where l.id=lj.id )
          """.stripMargin).show(1000)

  }

}
