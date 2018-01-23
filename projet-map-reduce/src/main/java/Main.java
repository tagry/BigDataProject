import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;

import com.twitter.chill.Tuple2LongLongSerializer;

public class Main {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("TP Spark");
		JavaSparkContext context = new JavaSparkContext(conf);

		long zoom = 1;
		if (args.length == 1) {
			zoom = Integer.parseInt(args[0]);
		}

		final long ZOOM = new Long(zoom);

		JavaPairRDD<String, PortableDataStream> rddFiles = context
				.binaryFiles("/dem3_raw/N00E006.hgt");

		System.out.println("<<<<<<<<<<<<< count rdd : " + rddFiles.count());

		System.out
				.println("<<<<<<<<<<<<<<<<<<<<<<<< avant >>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		JavaRDD<Map<String, Long>> rddHgtData = rddFiles
				.map((tuple) -> hgtConvertToClass(tuple._1, tuple._2, ZOOM));
		System.out.println("<<<<<<<<<<<<< count rdd : " + rddHgtData.count());
		List<Map<String, Long>> liste = rddHgtData.collect();
		System.out.println("<<<<<<<<<<<<< taille liste : " + liste.size());
		System.out.println("<<<<<<<<<<<<< taille map : " + liste.get(0).size());
		System.out
				.println("<<<<<<<<<<<<<<<<<<<<<<<<< apres >>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

		Map<String, Long> result = rddHgtData.reduce((Map<String, Long> m1,
				Map<String, Long> m2) -> reduceMap(m1, m2));

		storeMap(result, ZOOM);

		System.out
				.println("<<<"
						+ result.size()
						+ "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
		result.forEach((key, value) -> System.out.println(key + " " + value));

	}

	private static Map<String, Long> reduceMap(Map<String, Long> m1,
			Map<String, Long> m2) {

		m1.forEach((key, value) -> {
			if (checkSuperior(m2, key, value))
				m2.put(key, value);
		});

		return m2;
	}

	public static Map<String, Long> hgtConvertToClass(String filePath,
			PortableDataStream file, long zoom) {
		filePath = filePath.substring(filePath.length() - 11);

		Map<String, Long> mapPixels = new HashMap<String, Long>();

		DataInputStream data = file.open();

		String NS = filePath.substring(0, 1);
		String WE = filePath.substring(3, 4);

		long latitude = Integer.parseInt(filePath.substring(1, 3));
		long longitude = Integer.parseInt(filePath.substring(4, 7));

		if (NS.equals("N"))
			latitude = 90 - latitude;
		else
			latitude += 90;

		if (WE.equals("W"))
			longitude = 180 - longitude;
		else
			longitude += 180;

		try {

			// Pixel's coordinates
			Tuple2LongLongSerializer key;

			byte[] buf = new byte[2];

			for (int i = 0; i < 1201; i++) {
				for (int j = 0; j < 1201; j++) {
					buf[0] = data.readByte();
					buf[1] = data.readByte();

					long altitude = (buf[0] << 8) | buf[1];

					String pixelKey = getPixelKey(latitude, longitude, j, i,
							zoom);

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
		long step = 180 * 1201 / (zoom * 1024);
		long coordPixelX = (longitude * 1201 + coordX) / (step * 2);
		long coordPixelY = ((latitude - 1) * 1201 + coordY) / step;

		return coordPixelX + "-" + coordPixelY;
	}

	private static void storeMap(Map<String, Long> map, long zoom) {
		Map<String, Map<String, Long>> mapSplit = splitMap(map, zoom);

		mapSplit.forEach((k, v) -> HBaseConnector.addMap(
				"magrondin_lhing_tagry", new String("" + zoom), k, v));
	}

	private static Map<String, Map<String, Long>> splitMap(
			Map<String, Long> map, long zoom) {
		Map<String, Map<String, Long>> filesMap = new HashMap<String, Map<String, Long>>();

		map.forEach((k, v) -> {
			String[] keyTab = k.split("-");
			int x = Integer.parseInt(keyTab[0]);
			int y = Integer.parseInt(keyTab[1]);

			String fileKey = x / 1024 + "-" + y / 1024;

			String coordPixelInFile = x % 1024 + "-" + y % 1024;

			if (!filesMap.containsKey(fileKey))
				filesMap.put(fileKey, new HashMap<String, Long>());

			filesMap.get(fileKey).put(coordPixelInFile, v);
		});

		return filesMap;
	}
}
