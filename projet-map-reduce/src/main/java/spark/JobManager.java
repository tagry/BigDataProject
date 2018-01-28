package spark;

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

/**
 * Manage the spark job
 * 
 * @author maegrondin,lhing,tagry
 *
 */
public class JobManager {
	private final static int PIXELS_BY_IMAGE_SIDE = 1024;
	private final static int DATA_BY_FILE_SIDE = 1201;


	/**
	 * RDD containing all hgt files
	 * 
	 * @return (filesPath, files)
	 */
	public static JavaPairRDD<String, PortableDataStream> searchFiles(String filesPath, long zoom) {
		System.out.println("<<<<<<<<<<<<< START SEARCH FILES >>>>>>>>>>>>>>>>>");

		JavaPairRDD<String, PortableDataStream> rddFiles = Spark.context.binaryFiles(filesPath);

		System.out.println("<<<<<<<<<<<<< SEARCH FILES FINISHED >>>>>>>>>>>>>>>>>");

		return rddFiles;
	}

	/**
	 * RDD of Maps with: coordinates of pixel as String ("x-y") highest altitude of
	 * pixel as value Each map corresponding to a file.hgt
	 * 
	 * @param rddFiles
	 * @return Maps with: coordinates of pixel as String ("x-y") highest altitude
	 * 
	 */
	public static JavaRDD<Map<String, Long>> filesToMaps(JavaPairRDD<String, PortableDataStream> rddFiles, long zoom) {
		System.out.println("<<<<<<<<<<<<< START MAP >>>>>>>>>>>>>>>>>");
		JavaRDD<Map<String, Long>> rddHgtData = rddFiles.map((tuple) -> hgtFormatToMap(tuple._1, tuple._2, zoom));
		System.out.println("<<<<<<<<<<<<< MAP FINISHED >>>>>>>>>>>>>>>>>");

		return rddHgtData;
	}

	/**
	 * @param rddMap
	 * @return
	 */
	public static Map<String, Map<String, Long>> aggregatePixelsMaps(JavaRDD<Map<String, Long>> rddMap, long zoom) {
		System.out.println("<<<<<<<<<<<<< START AGREGATE >>>>>>>>>>>>>>>>>");
		Map<String, Map<String, Long>> init = new HashMap<>();
		Map<String, Map<String, Long>> result = rddMap.aggregate(init,
				(Map<String, Map<String, Long>> fF, Map<String, Long> iF) -> addPixelsToMapImages(fF, iF),
				(Map<String, Map<String, Long>> fF1, Map<String, Map<String, Long>> fF2) -> mergeFinalForm(fF1, fF2));

		System.out.println("<<<<<<<<<<<<< AGREGATE FINISHED >>>>>>>>>>>>>>>>>");
		return result;
	}

	/**
	 * Use in the aggregate to merge files maps to final maps
	 * 
	 * @param finalForm
	 * @param initialForm
	 * @return
	 */
	private static Map<String, Map<String, Long>> addPixelsToMapImages(Map<String, Map<String, Long>> finalForm,
			Map<String, Long> initialForm) {

		initialForm.forEach((k, v) -> {
			String[] keyTab = k.split("-");
			int x = Integer.parseInt(keyTab[0]);
			int y = Integer.parseInt(keyTab[1]);

			// Get the coordinates of the file where the pixel will be
			String fileKey = x / PIXELS_BY_IMAGE_SIDE + "-" + y / PIXELS_BY_IMAGE_SIDE;

			// Change the pixel's coordinates so that it goes from 0 to BLOC_SIDE
			// in
			// the file
			String coordPixelInFile = x % PIXELS_BY_IMAGE_SIDE + "-" + y % PIXELS_BY_IMAGE_SIDE;

			if (!finalForm.containsKey(fileKey))
				finalForm.put(fileKey, new HashMap<String, Long>(1025 * 1025));

			if (mergingPredicat(finalForm.get(fileKey), coordPixelInFile, v))
				finalForm.get(fileKey).put(coordPixelInFile, v);
		});

		return finalForm;
	}

	/**
	 * Use in the aggregate to merge final maps
	 * 
	 * @param finalForm1
	 * @param finalForm2
	 * @return
	 */
	private static Map<String, Map<String, Long>> mergeFinalForm(Map<String, Map<String, Long>> finalForm1,
			Map<String, Map<String, Long>> finalForm2) {

		finalForm1.forEach((k, v) -> {
			if (finalForm2.containsKey(k))
				finalForm2.put(k, reduceMap(v, finalForm2.get(k)));
			else
				finalForm2.put(k, v);

		});

		return finalForm2;
	}

	/**
	 * Merge 2 maps with mergingPredicat property
	 * 
	 * @param m1
	 * @param m2
	 * @return
	 */
	private static Map<String, Long> reduceMap(Map<String, Long> m1, Map<String, Long> m2) {

		try {
			m1.forEach((key, value) -> {
				if (mergingPredicat(m2, key, value))
					m2.put(key, value);
			});
		} catch (OutOfMemoryError e) {
			System.err.println("<<<<<<<<<<<<<<<<<<<<<<< ERROR : " + m2.size());
		}

		return m2;
	}

	/**
	 * Converts file to map
	 * 
	 * @param filePath
	 *            use to know latitude and longitude
	 * @param file
	 *            hgt file
	 * @param zoom
	 * @return
	 */
	private static Map<String, Long> hgtFormatToMap(String filePath, PortableDataStream file, long zoom) {

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

			for (int i = 0; i < DATA_BY_FILE_SIDE; i++) {
				for (int j = 0; j < DATA_BY_FILE_SIDE; j++) {
					buf[0] = data.readByte();
					buf[1] = data.readByte();

					long altitude = (buf[0] << 8) | buf[1];

					// Get the pixel in which the coordinates are located
					String pixelKey = getPixelKey(latitude, longitude, j, i, zoom);

					// Change the altitude of the pixel if the new altitude is
					// higher than the last value
					if (mergingPredicat(mapPixels, pixelKey, altitude))
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

	/**
	 * Use to select the highest altitude in a pixel
	 * 
	 * @param map
	 * @param coord
	 *            pixel coordinate
	 * @param altitude
	 *            altitude from the other map
	 * @return True if the value need to be update
	 */
	private static boolean mergingPredicat(Map<String, Long> map, String coord, long altitude) {
		return !map.containsKey(coord) || map.containsKey(coord) && map.get(coord) < altitude;
	}

	/**
	 * @param latitude
	 * @param longitude
	 * @param coordX
	 * @param coordY
	 * @param zoom
	 * @return The pixel coordinate in the full image
	 */
	private static String getPixelKey(long latitude, long longitude, long coordX, long coordY, long zoom) {
		long nbFilesBySide = (long) Math.pow(2, zoom);
		long nbPixelsBySide = (long) nbFilesBySide * PIXELS_BY_IMAGE_SIDE;

		long step = 180 * DATA_BY_FILE_SIDE / nbPixelsBySide; // nb points by side for one
		// pixel
		long coordPixelX = (longitude * DATA_BY_FILE_SIDE + coordX) / (step * 2);
		long coordPixelY = ((latitude - 1) * DATA_BY_FILE_SIDE + coordY) / step;

		return coordPixelX + "-" + coordPixelY;
	}
}
