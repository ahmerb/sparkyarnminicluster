package sparkyarnminicluster;

import java.util.Arrays;
import java.util.Comparator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class WordCount {

    public static long runWordCountJob(JavaSparkContext jsc) {
        JavaPairRDD<String, Integer> rdd = jsc
            .textFile("alice29.txt")
            .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
            .mapToPair(word -> new Tuple2<String, Integer>(word, 1))
            .reduceByKey((count1, count2) -> (int) count1 + (int) count2);

        return rdd.values().max(Comparator.naturalOrder());
    }

    public static void main(String[] args) {
        System.out.println("***** Started :D");
        // File file = new File("./hi.txt");
        // System.out.println(file.getAbsolutePath());
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("Word Count Application");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        long count = runWordCountJob(jsc);
        System.out.println("***** Max count: " + count);
        System.out.println("***** Done :D");
    }
}
