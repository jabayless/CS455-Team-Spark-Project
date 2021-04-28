
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.lower;

public class Joiner {
    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSession.builder().appName("Java Spark Example").getOrCreate();
        Dataset<Row> stations = spark.read()
                .option("header", "true")
                .option("ignoreLeadingWhiteSpace", true)
                .option("ignoreTrailingWhiteSpace", true)
                .csv("hdfs://juneau:49666/spark/data/full-ghcnd-stations.csv");
        Dataset<Row> cities = spark.read()
                .option("header", "true")
                .option("ignoreLeadingWhiteSpace", true)
                .option("ignoreTrailingWhiteSpace", true)
                .csv("hdfs://juneau:49666/spark/data/uscities.csv");
        Dataset<Row> data = spark.read()
                .option("header", "true")
                .option("ignoreLeadingWhiteSpace", true)
                .option("ignoreTrailingWhiteSpace", true)
                .csv("hdfs://juneau:49666/spark/data/max_min.csv")
                .withColumnRenamed("STATION_ID", "station");
        Dataset<Row> colIndex = spark.read()
                .option("header", "true")
                .option("ignoreLeadingWhiteSpace", true)
                .option("ignoreTrailingWhiteSpace", true)
                .csv("hdfs://juneau:49666/spark/data/colindex.csv")
                .withColumnRenamed("City", "city_name");
        Dataset<Row> hhIncome = spark.read()
                .option("header", "true")
                .option("ignoreLeadingWhiteSpace", true)
                .option("ignoreTrailingWhiteSpace", true)
                .csv("hdfs://juneau:49666/spark/data/hhincome.csv");
        Dataset<Row> popDensity = spark.read()
                .option("header", "true")
                .option("ignoreLeadingWhiteSpace", true)
                .option("ignoreTrailingWhiteSpace", true)
                .csv("hdfs://juneau:49666/spark/data/uscitypopdensity.csv")
                .withColumnRenamed("City", "city_name")
                .withColumnRenamed("Population Density (Persons/Square Mile)", "Pop. Density");

        Dataset<Row> truncCities = cities.drop("city_ascii", "county_fips", "county_name", "lat", "lng", "density", "source", "military", "incorporated", "timezone", "ranking", "zips", "id")
                                         .withColumnRenamed("state_id", "state_abbrv");
        Dataset<Row> dataCities = stations.join(data,
                stations.col("STATION_ID").equalTo(data.col("station"))
        ).drop("station", "ELEVATION", "LNG", "LAT");


        Dataset<Row> dataCitiesStates = dataCities.join(truncCities,
                lower(dataCities.col("STATION_NAME")).contains(lower(truncCities.col("city")))
                        .and(dataCities.col("STATE_ID").equalTo(truncCities.col("state_abbrv")))
        ).drop("state_abbrv");

        Dataset<Row> dataCitiesPop = dataCitiesStates.join(popDensity,
                lower(dataCitiesStates.col("city")).equalTo(lower(popDensity.col("city_name")))
                        .and(lower(dataCitiesStates.col("state_name")).equalTo(lower(popDensity.col("State"))))
        ).drop("Index", "State", "2016 Population", "Land Area (Square Miles)", "city_name");


        Dataset<Row> dataCitiesPopCol = dataCitiesPop.join(colIndex,
                dataCitiesPop.col("city").equalTo(colIndex.col("city_name"))
                        .and(dataCitiesPop.col("state_id").equalTo(colIndex.col("State")))
        ).drop("State", "city_name");


        Dataset<Row> completeJoin = dataCitiesPopCol.join(hhIncome,
                dataCitiesPopCol.col("state_name").equalTo(hhIncome.col("State"))
        ).drop("State").withColumnRenamed("Median", "Median House Income");

        completeJoin.printSchema();
        completeJoin.show(500);

        completeJoin.coalesce(1).write().option("header", true).csv("hdfs://juneau:49666/spark/out");

        spark.close();
    }
}
