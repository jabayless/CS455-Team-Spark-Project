import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;


public class SumAndCount {

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: SumAndCount <input file>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("SumAndCount");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> data = ctx.textFile(args[0], 1);

        String delim = ",";

        // Map
        JavaPairRDD<String, String> values = data.mapToPair (line ->
                {
                    String[] columns = line.split(",");
                    String key = (columns[0]);
                    if (!columns[2].equals("TAVG")) return new Tuple2<>("NA", "0,0");
                    String value = (columns[3] + delim + "1");
                    return new Tuple2<>(key, value);
                }
        );

//        // Reduce and print
//        List<Tuple2<String, String>> results = values.reduceByKey((x, y) ->
//                {
//                    String[] lhs = x.split(delim);
//                    String[] rhs = y.split(delim);
//                    String sum = Integer.valueOf(lhs[0]) + Integer.valueOf(rhs[0]) + "";
//                    String count = Integer.valueOf(lhs[1]) + Integer.valueOf(rhs[1]) + "";
//                    return sum + delim + count;
//                }
//        ).collect();
//
//        for (Tuple2<?, ?> tuple : results) {
//            System.out.println(tuple._1() + ": " + tuple._2());
//        }

//
        // Reduce and save
        JavaPairRDD<String, String> results = values.reduceByKey((x, y) ->
                {
                    String[] lhs = x.split(delim);
                    String[] rhs = y.split(delim);
                    String sum = Integer.valueOf(lhs[0]) + Integer.valueOf(rhs[0]) + "";
                    String count = Integer.valueOf(lhs[1]) + Integer.valueOf(rhs[1]) + "";
                    return sum + delim + count;
                }
            );

        results.saveAsTextFile("file:///s/bach/g/under/zwikel/cs/455/project/output");

        ctx.stop();
    }

}
