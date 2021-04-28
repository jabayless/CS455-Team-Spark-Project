import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.lower;

public class Joiner {
    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSession.builder().appName("Java Spark Example").getOrCreate();
        Dataset<Row> states = spark.read()
                .option("header", "true")
                .option("ignoreLeadingWhiteSpace", true)
                .option("ignoreTrailingWhiteSpace", true)
                .csv("hdfs://juneau:49666/spark/data/states.csv");
        Dataset<Row> stations = spark.read()
                .option("header", "true")
                .option("ignoreLeadingWhiteSpace", true)
                .option("ignoreTrailingWhiteSpace", true)
                .csv("hdfs://juneau:49666/spark/data/stations_condensed.csv");
        Dataset<Row> cities = spark.read()
                .option("header", "true")
                .option("ignoreLeadingWhiteSpace", true)
                .option("ignoreTrailingWhiteSpace", true)
                .csv("hdfs://juneau:49666/spark/data/uscities.csv");

        Dataset<Row> truncCities = cities.drop("city_ascii", "county_fips", "county_name", "lat", "lng", "population", "density", "source", "military", "incorporated", "timezone", "ranking", "zips", "id");
        Dataset<Row> stateStations = stations.withColumn("STATE_ID", expr("substr(STATION_ID, 4, 2)"));
        Dataset<Row> uniqueStations = stateStations.dropDuplicates("STATION_NAME");
        Dataset<Row> joined = uniqueStations.join(truncCities,
                lower(uniqueStations.col("STATION_NAME")).equalTo(lower(truncCities.col("city")))
                        .and(uniqueStations.col("STATE_ID").equalTo(truncCities.col("state_id")))
        );

        states.printSchema();
        stations.printSchema();
        cities.printSchema();
        truncCities.printSchema();

        joined.show();

        spark.close();
    }
}
