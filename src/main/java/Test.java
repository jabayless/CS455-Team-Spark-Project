import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class Test {

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
//        uniqueStations.show();
//        truncCities.show();

        spark.close();


//        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount");
//        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
//        JavaRDD<String> lines = ctx.textFile(args[0], 1);
//
//        JavaRDD<String> words
//                = lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
//        JavaPairRDD<String, Integer> ones
//                = words.mapToPair(word -> new Tuple2<>(word, 1));
//        JavaPairRDD<String, Integer> counts
//                = ones.reduceByKey((Integer i1, Integer i2) -> i1 + i2);
//
//        List<Tuple2<String, Integer>> output = counts.collect();
//        for (Tuple2<?, ?> tuple : output) {
//            System.out.println(tuple._1() + ": " + tuple._2());
//        }
//        ctx.stop();
    }

}
