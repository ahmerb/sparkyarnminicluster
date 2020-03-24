package sparkyarnminicluster;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import com.holdenkarau.spark.testing.YARNCluster;

public class FailsOnClusterJobTest {
    public static YARNCluster yarnCluster = null;
    public static JavaSparkContext jsc = null;

    @BeforeClass
    public static void beforeClass() {
        yarnCluster = new YARNCluster();
        yarnCluster.startYARN();

        SparkConf conf = new SparkConf().setMaster("yarn-client").setAppName("Fails On Cluster Word Count Application Test with YARN MiniCluster");
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
    public void testFailsOnClusterJobWithYarnCluster() {
        long count = FailsOnClusterJob.runFailsOnClusterJob(jsc);
        assertEquals("Max count is 6061", 6061, count);
    }
}
