package sparkyarnminicluster;

import java.util.Arrays;
import java.util.Comparator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * A job that works when running in standalone mode and fails when running on YARN cluster.
 * Run {@link FailsOnClusterJob#main(String[])} to run in standalone mode with 4 cores.
 * Run the tests to run on YARN MiniCluster.
 */
public class FailsOnClusterJob {
    private static int myNumber = 10;

    public static class AddFunction {
        public int number;

        public AddFunction(int number) {
            this.number = number;
        }

        public int add(int x) {
            return x + number;
        }
    }

    public static long runFailsOnClusterJob(JavaSparkContext jsc) {
        JavaPairRDD<String, Integer> rdd = jsc
            .textFile("alice29.txt")
            .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
            .mapToPair(word -> new Tuple2<String, Integer>(word, 1))
            .reduceByKey((count1, count2) -> (int) count1 + (int) count2)
            .mapValues(count -> new AddFunction(myNumber).add(count)); // FAILS

        return rdd.values().max(Comparator.naturalOrder());
    }

    public static void main(String[] args) {
        System.out.println("***** Started FailsOnClusterJob :D");
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("Fails on Cluster Word Count Application");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        long count = runFailsOnClusterJob(jsc);
        System.out.println("***** Max count: " + count);
        System.out.println("***** Done :D");
    }
}
