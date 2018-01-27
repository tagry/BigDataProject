package spark;

import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;

public class SparkManager {
	private SparkConf conf = new SparkConf().setAppName("TP Spark");
	private JavaSparkContext context = new JavaSparkContext(conf);

	private String path;

	public SparkManager(String path) {
		this.path = path;
	}

	private JavaPairRDD<String, PortableDataStream> searchFiles() {
		return null;
	}

	private JavaRDD<Map<String, Long>> handleFiles(JavaPairRDD<String, PortableDataStream> files) {
		return null;
	}

	private Map<String, Map<String, Long>> aggregatePixelsMaps(JavaRDD<Map<String, Long>> pixelsMaps) {
		return null;
	}
}
