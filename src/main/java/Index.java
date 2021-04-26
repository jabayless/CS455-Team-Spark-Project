import com.clearspring.analytics.util.Pair;
import org.apache.hadoop.util.hash.Hash;

import java.util.*;

public class Index<T extends Number & Comparable<T>> {
    /*
    ==================
        How to use
    ==================
    Creation:
        new Index<T extends Number & Comparable>();

    Adding Values:
        index.addValue(String id, Number value);

    Finding Z-Score:
        Double zScore = index.getZScore(value);

    Getting all Z-Scores as List:
        List<Pair<Double>> zScoreList = index.getZScores();

    Combining Z-Score Maps (static):
        Index.combineZScores(Map<String, Double>...)

    Other:
      You can also find the mean directly or find the std dev:
        index.findMean()
        index.findStandardDev()

     */

    public static class Pair<V extends Number & Comparable<V>> implements Comparable<Pair<V>> {
        public final String id;
        public final V value;
        public Pair(String id, V value) { this.id=id; this.value=value; }

        @Override
        public int compareTo(Pair<V> p) {
            return this.value.compareTo(p.value);
        }

        @Override
        public String toString() {
            return "<" + id + ", " + value + ">";
        }
    }

    private Double total = 0.0d;
    private Double mean = 0.0d;
    private Double standardDev = 0.0d;
    private final List<Pair<T>> values = new ArrayList<>();

    public double findMean() {
        mean = total / values.size();
        return mean;
    }

    public double findStandardDev() {
        if(mean.equals(0.0d)) {
            findMean();
        }

        double sumAbsXMinusMeanSq = 0.0f;
        for(Pair<T> entry: values) {
            double xMinusMean = (Double) entry.value - mean;
            double absXMinusMean = Math.abs(xMinusMean);
            sumAbsXMinusMeanSq += Math.pow(absXMinusMean, 2);
        }

        double sumOverN = sumAbsXMinusMeanSq / values.size();
        standardDev = Math.sqrt(sumOverN);
        return standardDev;
    }

    public double getZScore(T value) {
        if(standardDev.equals(0.0d)) {
            findStandardDev();
        }

        double xMinusMean = (Double) value - mean;
        return xMinusMean / standardDev;
    }

    public double getZScoreById(String id) {
        for(Pair<T> pair: values) {
            if(pair.id.equals(id)) {
                return getZScore(pair.value);
            }
        }
        return 0.0f;
    }

    public List<Pair<Double>> getZScores() {
        List<Pair<Double>> zScores = new ArrayList<>();
        for(Pair<T> pair : values) {
            zScores.add(new Pair(pair.id, getZScore(pair.value)));
        }
        Collections.sort(zScores);
        return zScores;
    }

    public void addValue(String id, T value) {
        total += (Double) value;
        System.out.println((Double) value);
        values.add(new Pair<T>(id, value));
    }

    /*
        - I know this looks inefficient but this is O(2n) instead of O(N) of using
          only lists to combine because of time complexity of the lookup.
        - Using max value as the default to fix skewing of values not in each table
     */
    public static List<Pair<Double>> combineZScores(List<Pair<Double>>... zScoreLists) {
        Map<String, Double> combinedZScoreMap = new HashMap<>();
        for(List<Pair<Double>> list: zScoreLists) {
            for(Pair<Double> pair : list) {
                double sumByKey = combinedZScoreMap.getOrDefault(pair.id, Double.MAX_VALUE);
                sumByKey += pair.value;
                combinedZScoreMap.put(pair.id, sumByKey);
            }
        }
        List<Pair<Double>> combinedZScoreList = new ArrayList<>();
        for(Map.Entry<String, Double> entry: combinedZScoreMap.entrySet()) {
            combinedZScoreList.add(new Pair<>(entry.getKey(), entry.getValue()));
        }
        return combinedZScoreList;
    }

    public static void main(String[] args) {
        Index<Double> happiness = new Index<>();
        happiness.addValue("Dallas", 5.0d);
        happiness.addValue("Detroit", 7.0d);
        happiness.addValue("Miami", 6.3d);
        happiness.addValue("Denver", 5.1d);
        happiness.addValue("Tokyo", 4.2d);
        happiness.addValue("New York", 6.9d);
        happiness.addValue("Tuscaloosa", 3.4d);
        happiness.addValue("Mobile", 8.5d);
        happiness.addValue("Chicago", 7.1d);

        System.out.println(happiness.findStandardDev());
        System.out.println(happiness.findMean());
        List<Pair<Double>> zScores = happiness.getZScores();
        System.out.println(zScores);
    }
}
