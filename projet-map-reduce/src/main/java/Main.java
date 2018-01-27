import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;

import com.google.protobuf.DescriptorProtos.SourceCodeInfo.Location;

public class Main {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("TP Spark");
		JavaSparkContext context = new JavaSparkContext(conf);

		if (args.length < 3) {
			System.out
					.println("Usage : <spark-submit --master yarn --driver-cores 10 --executor-memory 6000M "
							+ "--num-executors 10 --jars $JARS --class Main target/TPSpark-0.0.1.jar "
							+ "<zoom> <files location> <HBase tableName>");
			return;
		}

		/**
		 * Get the zoom from the command line
		 */
		long zoom = 0;
		if (args.length >= 1) {
			zoom = Integer.parseInt(args[0]);
		}
		final long ZOOM = new Long(zoom);

		/**
		 * Get the location of hgt files on HDFS
		 */
		// /user/raw_data/dem3/* /user/tagry/hgtData/* /dem3_raw/*
		String path = "/user/raw_data/dem3/*";
		if (args.length >= 2) {
			path = args[1];
		}

		/**
		 * Get the database to use
		 */
		String bdd = "maegrondin_lhing_tagry_default";
		if (args.length >= 3) {
			bdd = args[2];
		}

		/**
		 * RDD containing all hgt files
		 */

		System.out.println("<<<<<<<<<<<<< SEARCH FILES >>>>>>>>>>>>>>>>>");

		JavaPairRDD<String, PortableDataStream> rddFiles = context
				.binaryFiles(path);

		System.out.println("<<<<<<<<<<<<< count rdd : " + rddFiles.count());

		/**
		 * RDD of Maps with: coordinates of pixel as String ("x-y") highest
		 * altitude of pixel as value Each map corresponding to a file.hgt
		 */
		System.out.println("<<<<<<<<<<<<< START MAP >>>>>>>>>>>>>>>>>");
		JavaRDD<Map<String, Long>> rddHgtData = rddFiles
				.map((tuple) -> hgtConvertToClass(tuple._1, tuple._2, ZOOM));
		System.out.println("<<<<<<<<<<<<< MAP FINISHED >>>>>>>>>>>>>>>>>");

		System.out.println("<<<<<<<<<<<<< START AGREGATE >>>>>>>>>>>>>>>>>");
		Map<String, Map<String, Long>> init = new HashMap<>();
		Map<String, Map<String, Long>> result = rddHgtData
				.aggregate(
						init,
						(Map<String, Map<String, Long>> fF, Map<String, Long> iF) -> addPixelsToImages(
								fF, iF),
						(Map<String, Map<String, Long>> fF1,
								Map<String, Map<String, Long>> fF2) -> mergeFinalForm(
								fF1, fF2));

		System.out.println("<<<<<<<<<<<<< AGREGATE FINISHED >>>>>>>>>>>>>>>>>");

		// /**
		// * Merge of all maps
		// */
		// System.out.println("<<<<<<<<<<<<< START REDUCE >>>>>>>>>>>>>>>>>");
		// Map<String, Long> result = rddHgtData.reduce((Map<String, Long> m1,
		// Map<String, Long> m2) -> reduceMap(m1, m2));
		// System.out.println("<<<<<<<<<<<<< REDUCE FINISHED >>>>>>>>>>>>>>>>>");
		// /**
		// * Splits the final map into several maps according to the zoom and
		// stores them into HBase
		// */

		System.out.println("<<<<<<<<<<<<< START STORAGE >>>>>>>>>>>>>>>>>");
		storeMap(result, ZOOM, bdd);
		System.out.println("<<<<<<<<<<<<< STORAGE DONE >>>>>>>>>>>>>>>>>");

		System.out.println("<<<" + result.size() + "<<<<<<<<<<<<<<<<<<<<<<");

		context.close();
	}

	public static Map<String, Map<String, Long>> addPixelsToImages(
			Map<String, Map<String, Long>> finalForm,
			Map<String, Long> initialForm) {

		initialForm.forEach((k, v) -> {
			String[] keyTab = k.split("-");
			int x = Integer.parseInt(keyTab[0]);
			int y = Integer.parseInt(keyTab[1]);

			// Get the coordinates of the file where the pixel will be
				String fileKey = x / 1024 + "-" + y / 1024;

				// Change the pixel's coordinates so that it goes from 0 to 1024
				// in
				// the file
				String coordPixelInFile = x % 1024 + "-" + y % 1024;

				if (!finalForm.containsKey(fileKey))
					finalForm.put(fileKey, new HashMap<String, Long>(
							1025 * 1025));

				if (checkSuperior(finalForm.get(fileKey), coordPixelInFile, v))
					finalForm.get(fileKey).put(coordPixelInFile, v);
			});

		return finalForm;
	}

	public static Map<String, Map<String, Long>> mergeFinalForm(
			Map<String, Map<String, Long>> finalForm1,
			Map<String, Map<String, Long>> finalForm2) {

		finalForm1.forEach((k, v) -> {
			if (finalForm2.containsKey(k))
				finalForm2.put(k, reduceMap(v, finalForm2.get(k)));
			else
				finalForm2.put(k, v);

		});

		return finalForm2;
	}

	private static Map<String, Long> reduceMap(Map<String, Long> m1,
			Map<String, Long> m2) {

		try{
		m1.forEach((key, value) -> {
			if (checkSuperior(m2, key, value))
				m2.put(key, value);
		});
		}
		catch(OutOfMemoryError e){
			System.out.println("<<<<<<<<<<<<<<<<<<<<<<< ERROR : " + m2.size());
		}

		

		return m2;
	}

	public static Map<String, Long> hgtConvertToClass(String filePath,
			PortableDataStream file, long zoom) {

		filePath = filePath.substring(filePath.length() - 11);

		// Eliminates non correct files
		Pattern pattern = Pattern.compile("(N|S)\\d{2}(W|E)\\d{3}.hgt\\z");
		Matcher matcher = pattern.matcher(filePath);

		if (!matcher.find())
			return new HashMap<String, Long>();

		Map<String, Long> mapPixels = new HashMap<String, Long>();

		DataInputStream data = file.open();

		// Get latitude and longitude from file name
		long latitude = Integer.parseInt(filePath.substring(1, 3));
		long longitude = Integer.parseInt(filePath.substring(4, 7));

		// Change coordinates of hgt file so that it goes from 0 to 180 for
		// latitude
		// and from 0 to 360 for longitude
		String NS = filePath.substring(0, 1);
		String WE = filePath.substring(3, 4);

		if (NS.equals("N"))
			latitude = 90 - latitude;
		else
			latitude += 90;
		if (WE.equals("W"))
			longitude = 180 - longitude;
		else
			longitude += 180;

		try {

			byte[] buf = new byte[2];

			for (int i = 0; i < 1201; i++) {
				for (int j = 0; j < 1201; j++) {
					buf[0] = data.readByte();
					buf[1] = data.readByte();

					long altitude = (buf[0] << 8) | buf[1];

					// Get the pixel in which the coordinates are located
					String pixelKey = getPixelKey(latitude, longitude, j, i,
							zoom);

					// Change the altitude of the pixel if the new altitude is
					// higher than the last value
					if (checkSuperior(mapPixels, pixelKey, altitude))
						mapPixels.put(pixelKey, altitude);
				}
			}
			data.close();

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return mapPixels;
	}

	private static boolean checkSuperior(Map<String, Long> map, String coord,
			long altitude) {
		return !map.containsKey(coord) || map.containsKey(coord)
				&& map.get(coord) < altitude;
	}

	private static String getPixelKey(long latitude, long longitude,
			long coordX, long coordY, long zoom) {
		long nbFilesBySide = (long) Math.pow(2, zoom);
		long nbPixelsBySide = (long) nbFilesBySide * 1024;

		long step = 180 * 1201 / nbPixelsBySide; // nb points by side for one
													// pixel
		long coordPixelX = (longitude * 1201 + coordX) / (step * 2);
		long coordPixelY = ((latitude - 1) * 1201 + coordY) / step;

		return coordPixelX + "-" + coordPixelY;
	}

	private static String[] generateFamilies(int maxZoom) {
		String[] result = new String[maxZoom];

		for (int i = 0; i < maxZoom; i++)
			result[i] = i + "";

		return result;
	}

	private static void storeMap(Map<String, Map<String, Long>> map, long zoom,
			String bdd) {
		String tableName = bdd;
		int maxZoom = 5;

		// Create table if it does not exist
		HBaseConnector.initHBase(tableName, generateFamilies(maxZoom), zoom);

		int nbImagePerSide = (int) Math.pow(2, zoom);

		for (int i = 0; i < nbImagePerSide; i++)
			for (int j = 0; j < nbImagePerSide; j++) {
				String key = j + "-" + i;
				if (map.containsKey(key))
					HBaseConnector.addMap(tableName, key,
							new String("" + zoom), map.get(key));
				else
					HBaseConnector.addMap(tableName, key,
							new String("" + zoom), new HashMap<String, Long>());
			}

	}

	// private static Map<String, Map<String, Long>> splitMap(
	// Map<String, Long> map, long zoom) {
	// Map<String, Map<String, Long>> filesMap = new HashMap<String, Map<String,
	// Long>>();
	//
	// map.forEach((k, v) -> {
	// String[] keyTab = k.split("-");
	// int x = Integer.parseInt(keyTab[0]);
	// int y = Integer.parseInt(keyTab[1]);
	//
	// // Get the coordinates of the file where the pixel will be
	// String fileKey = x / 1024 + "-" + y / 1024;
	//
	// // Change the pixel's coordinates so that it goes from 0 to 1024 in
	// // the file
	// String coordPixelInFile = x % 1024 + "-" + y % 1024;
	//
	// if (!filesMap.containsKey(fileKey))
	// filesMap.put(fileKey, new HashMap<String, Long>());
	//
	// filesMap.get(fileKey).put(coordPixelInFile, v);
	// });
	//
	// return filesMap;
	// }
}
