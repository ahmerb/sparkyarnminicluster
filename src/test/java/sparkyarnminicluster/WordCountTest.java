package sparkyarnminicluster;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import com.holdenkarau.spark.testing.YARNCluster;

public class WordCountTest {
    public static YARNCluster yarnCluster = null;
    public static JavaSparkContext jsc = null;

    @BeforeClass
    public static void beforeClass() {
        yarnCluster = new YARNCluster();
        yarnCluster.startYARN();

        SparkConf conf = new SparkConf().setMaster("yarn-client").setAppName("Word Count Application Test with YARN MiniCluster");
        jsc = new JavaSparkContext(conf);
    }

    @AfterClass
    public static void afterClass() {
        if (jsc != null) {
            jsc.stop();
        }
        if (yarnCluster != null) {
            yarnCluster.shutdownYARN();
        }
    }

    @Test
    public void testWordCountJobWithYarnCluster() {
        long count = WordCount.runWordCountJob(jsc);
        assertEquals("Max count is 6051", 6051, count);
    }
}
