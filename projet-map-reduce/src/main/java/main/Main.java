package main;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;

import hbase.HBaseManager;
import spark.JobManager;

/**
 * @author maegrondin,lhing,tagry
 *
 */
public class Main {
	private static long zoom = 0;

	// /user/raw_data/dem3/* /user/tagry/hgtData/* /dem3_raw/*
	private static String path = "/user/raw_data/dem3/*";

	private static String tableName = "maegrondin_lhing_tagry_default";

	public static void main(String[] args) {
		setParameters(args);
		
		JobManager job = new JobManager(path, zoom);
		Map<String, Map<String, Long>> result = job.startJob();

		HBaseManager storageManager = new HBaseManager(tableName);
		storageManager.storeMap(result, zoom);
	}

	/**
	 * @param args
	 */
	private static void setParameters(String[] args) {
		if (args.length < 3) {
			System.out.println("Usage : <spark-submit --master yarn --driver-cores 10 --executor-memory 6000M "
					+ "--num-executors 10 --jars $JARS --class Main target/TPSpark-0.0.1.jar "
					+ "<zoom> <files location> <HBase tableName>");
			return;
		}

		/**
		 * Get the zoom from the command line
		 */
		if (args.length >= 1) {
			zoom = Integer.parseInt(args[0]);
		}

		/**
		 * Get the location of hgt files on HDFS
		 */
		if (args.length >= 2) {
			path = args[1];
		}

		/**
		 * Get the database to use
		 */
		if (args.length >= 3) {
			tableName = args[2];
		}
	}
}
