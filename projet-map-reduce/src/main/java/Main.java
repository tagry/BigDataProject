import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
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

		// JavaRDD<Integer> rdd;
		// rdd = context.parallelize(Arrays.asList(1, 2, 8, 7, 2),10);

		JavaPairRDD<String, PortableDataStream> rddFiles = context
				.binaryFiles("/dem3_raw/N00E033.hgt");

		JavaRDD<Map<String, Long>> rddHgtData = rddFiles
				.map((tuple) -> hgtConvertToClass(tuple._1, tuple._2));

		Map<String, Long> result = rddHgtData.reduce((Map<String, Long> m1,
				Map<String, Long> m2) -> reduceMap(m1, m2));
		
		System.out.println("<<<"+ result.size()+ "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
		result.forEach((key, value)-> System.out.println(key + " " + value));
		

		// JavaPairRDD<Tuple2IntIntSerializer, Integer> rddPixelKey =
		// rddHgtData.keyBy(data -> generateKey(data));

//		JavaRDD<String> rddToString = rddHgtData.map(data -> data.toString());
//		rddToString
//				.saveAsTextFile("/user/lhing/aaaaaaaaaaaaaaaaaaaaaaa0000000000000000");

	}

	private static Map<String, Long> reduceMap(Map<String, Long> m1,
			Map<String, Long> m2) {
		
		m1.forEach((key, value)->{
			if(checkSuperior(m2, key, value))
				m2.put(key, value);
		});
		
		return m2;
	}

	public static Map<String, Long> hgtConvertToClass(String filePath,
			PortableDataStream file) {

		Map<String, Long> mapPixels = new HashMap();

		DataInputStream data = file.open();

		/**
		 * TODO pass zoom in parameter
		 */
		long zoom = 1;

		System.out.println(">>>>>>>>>>>>>>>>> FILE PATH : " + filePath);

		String NS = filePath.substring(29, 30);
		String WE = filePath.substring(32, 33);

		long latitude = Integer.parseInt(filePath.substring(30, 32));
		long longitude = Integer.parseInt(filePath.substring(33, 36));

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

			char[] buf = new char[2];

			for (int i = 0; i < 1201; i++) {
				for (int j = 0; j < 1201; j++) {
					buf[0] = data.readChar();
					buf[1] = data.readChar();

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
		return map.containsKey(coord) && map.get(coord) < altitude;
	}

	private static String getPixelKey(long latitude, long longitude,
			long coordX, long coordY, long zoom) {
		long step = 180 * 1201 / (zoom * 1024);
		long coordPixelX = (longitude * 1201 + coordX) / (step * 2);
		long coordPixelY = ((latitude - 1) * 1201 + coordY) / step;

		return coordPixelX + "-" + coordPixelY;
	}

}
