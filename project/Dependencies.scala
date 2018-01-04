import sbt._

object Dependencies {

  val sparkVersion  = "2.1.0"
  val akkaVersion   = "2.4.12"

  val sparkCore       = "org.apache.spark"            %% "spark-core"           % sparkVersion
  val sparkSql        = "org.apache.spark"            %% "spark-sql"            % sparkVersion
  val sparkExamples   = "org.apache.spark"            %% "spark-examples"       % sparkVersion
  val sparkml         = "org.apache.spark"            %% "spark-mllib"          % sparkVersion
  val akkaActor       = "com.typesafe.akka"           %% "akka-actor"           % akkaVersion
  val akkaSlf4j       = "com.typesafe.akka"           %% "akka-slf4j"           % akkaVersion
  val typesafeConfig  = "com.typesafe"                % "config"                % "1.3.0"
  val mysql           = "mysql"                       % "mysql-connector-java"  % "5.1.39"
  val sparkRedshift   = "com.databricks"              %% "spark-redshift"       % "2.0.1"
  val sparkCsv        = "com.databricks"              %% "spark-csv"            % "1.5.0"
  val sparkTs         = "com.cloudera.sparkts"        % "sparkts"               % "0.4.1"
  val slf4jlog4j      = "org.slf4j"                   % "slf4j-log4j12"         % "1.7.21"
  val postgres        = "org.postgresql"              % "postgresql"            % "9.4-1200-jdbc41" exclude("org.slf4j", "slf4j-simple")
  val scalatest       = "org.scalatest"               %% "scalatest"            % "3.0.1"
  val akkaTestkit     = "com.typesafe.akka"           %% "akka-testkit"         % akkaVersion
  val h2              = "com.h2database"              % "h2"                    % "1.4.192"
  val commonsIO       = "commons-io"                  % "commons-io"            % "2.5"
  val htmlCleaner     = "net.sourceforge.htmlcleaner" % "htmlcleaner"           % "2.19"
  val pdfbox          = "org.apache.pdfbox"           % "pdfbox"                % "2.0.4"
  val pdfbox_tools    = "org.apache.pdfbox"           % "pdfbox-tools"          % "2.0.4"
  val bcprov          = "org.bouncycastle" % "bcprov-jdk15on" % "1.54"
  val bcmail          = "org.bouncycastle" % "bcmail-jdk15on" % "1.54"
  val bcpkix          = "org.bouncycastle" % "bcpkix-jdk15on" % "1.54"
  val scalaopt        = "com.github.scopt" %% "scopt" % "3.5.0"
  val es              = "org.elasticsearch" % "elasticsearch-spark-20_2.11" % "5.6.1"


}