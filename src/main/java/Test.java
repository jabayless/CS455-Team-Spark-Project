import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Test {

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: JavaWordCount <file>");
            System.exit(1);
        }

        SparkSession spark = SparkSession.builder().appName("Java Spark Example").getOrCreate();
        DataFrameReader dataFrameReader = spark.read();
        Dataset<Row> dataFrame = dataFrameReader.option("header", "true")
                                                .option("ignoreLeadingWhiteSpace", "true")
                                                .option("ignoreTrailingWhiteSpace", "true")
                                                .option("mode", "DROPMALFORMED")
                                                .csv("hdfs://juneau:49666/data/1996.csv");

        // path is found in core-site.xml
        dataFrame.sample(1/1000.0).write().csv("hdfs://juneau:49666/home/spark/1996.csv");
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
