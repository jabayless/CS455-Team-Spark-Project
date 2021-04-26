import com.clearspring.analytics.util.Pair;
import org.apache.hadoop.util.hash.Hash;

import java.util.*;

public class Index<T extends Number> {
    /*
    ==================
        How to use
    ==================
    Creation:
        new Index<[type of value]>();

    Adding Values:
        index.addValue([value]);

    Finding Z-Score:
        index.getZScore([value]);

    Getting all Z-Scores as List:
        index.getZScores();

     */
    public static class Pair<T extends Number, Comparable> {
        public T value;
        public String id;
        public Pair(String id, T value) { this.value=value; this.id=id; }
    }

    private Double total = 0.0d;
    private Double mean = 0.0d;
    private Double standardDev = 0.0d;
    private final Map<String, T> values = new HashMap<>();

    public double findMean() {
        mean = values.size() / total;
        return mean;
    }

    public double findStandardDev() {
        if(mean.equals(0.0d)) {
            findMean();
        }

        double sumAbsXMinusMeanSq = 0.0f;
        for(Map.Entry<String, T> entry: values.entrySet()) {
            double xMinusMean = (double) entry.getValue() - mean;
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

        double xMinusMean = (double) value - mean;
        return xMinusMean / standardDev;
    }

    public double getZScoreById(String id) {
        return getZScore(values.get(id));
    }

    public Map<String, Double> getZScores() {
        Map<String, Double> zScores = new HashMap<>();
        for(Map.Entry<String, T> entry: values.entrySet()) {
            zScores.put(entry.getKey(), getZScore(entry.getValue()));
        }
        return sortByValues(zScores);
    }

    public void addValue(String id, T value) {
        total += (double) value;
        values.put(id, value);
    }

    public Map<String, Double> combineZScores(Map<String, Double>... zScoreMaps) {
        Map<String, Double> combinedZScoreMap = new HashMap<>();
        for(Map<String, Double> map: zScoreMaps) {

        }
        return combinedZScoreMap;
    }

    public static void main(String[] args) {



    }

    // Author: Chaitanya Singh
    // https://beginnersbook.com/2014/07/how-to-sort-a-treemap-by-value-in-java/
    public static <K, V extends Comparable<V>>
    Map<K, V> sortByValues(final Map<K, V> map) {
        Comparator<K> valueComparator =
                new Comparator<K>() {
                    public int compare(K k1, K k2) {
                        int compare =
                                map.get(k1).compareTo(map.get(k2));
                        if (compare == 0)
                            return 1;
                        else
                            return compare;
                    }
                };

        Map<K, V> sortedByValues =
                new TreeMap<K, V>(valueComparator);
        sortedByValues.putAll(map);
        return sortedByValues;
    }

}
