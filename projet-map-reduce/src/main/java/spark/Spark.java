package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Spark {
	public final static SparkConf conf = new SparkConf().setAppName("TP Spark");
	public final static JavaSparkContext context = new JavaSparkContext(conf);
}
