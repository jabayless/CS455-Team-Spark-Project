
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
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
        Index.combineZScores(List<String, Double>...)

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
            return p.value.compareTo(this.value);
        }

        @Override
        public String toString() {
            return "<" + id + ", " + value + ">\n";
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
            zScores.add(new Pair<>(pair.id, getZScore(pair.value)));
        }
        Collections.sort(zScores);
        return zScores;
    }

    public void addValue(String id, T value) {
        total += (Double) value;
        values.add(new Pair<>(id, value));
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
                double sumByKey = combinedZScoreMap.getOrDefault(pair.id, 0.0);
                sumByKey += pair.value;
                combinedZScoreMap.put(pair.id, sumByKey);
            }
        }
        List<Pair<Double>> combinedZScoreList = new ArrayList<>();
        for(Map.Entry<String, Double> entry: combinedZScoreMap.entrySet()) {
            combinedZScoreList.add(new Pair<>(entry.getKey(), entry.getValue()));
        }
        Collections.sort(combinedZScoreList);
        return combinedZScoreList;
    }

    public static void main(String[] args) {
        List<List<String>> records = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader("../../cs/cs455/spark/complete.csv"))) {
            String line = br.readLine();
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                records.add(Arrays.asList(values));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        Index<Double> tempIndex = new Index<>();
        Index<Double> coliIndex = new Index<>();
        Index<Double> popdensIndex = new Index<>();
        Index<Double> hhiIndex = new Index<>();

        for(List<String> row: records) {
            String key = row.get(5) + " " + row.get(1) + " " + row.get(0);
            /*
                So here, we take the Division of Total and Count to get the Average.
                We take the difference between the Average and 70 degrees.
                Now we want to be able to sort by closest to 0, to do this,
                Take the absolute value
             */
            Double avg = -1 * Math.abs((Double.parseDouble(row.get(3))/ Double.parseDouble(row.get(4))) - 70.0);
            Double coli = -1 * Double.parseDouble(row.get(9));
            Double popdens = -1 * Double.parseDouble(row.get(8));
            Double hhi = Double.parseDouble(row.get(10));

            tempIndex.addValue(key, avg);
            coliIndex.addValue(key, coli);
            popdensIndex.addValue(key, popdens);
            hhiIndex.addValue(key, hhi);
        }

        List<Index.Pair<Double>> tempZScores = tempIndex.getZScores();
        List<Index.Pair<Double>> coliZScores = coliIndex.getZScores();
        List<Index.Pair<Double>> popdensZScores = popdensIndex.getZScores();
        List<Index.Pair<Double>> hhiZScores = hhiIndex.getZScores();

        List<Index.Pair<Double>> happinessZScores = Index.combineZScores(tempZScores, coliZScores, popdensZScores, hhiZScores);
        System.out.println(happinessZScores.toString());
    }
}
