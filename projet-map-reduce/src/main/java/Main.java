import java.io.DataInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;

import com.twitter.chill.Tuple2IntIntSerializer;

import scala.reflect.internal.Trees.Return;

public class Main {

	public static void main(String[] args) {

		

		SparkConf conf = new SparkConf().setAppName("TP Spark");
		JavaSparkContext context = new JavaSparkContext(conf);

		// JavaRDD<Integer> rdd;
		// rdd = context.parallelize(Arrays.asList(1, 2, 8, 7, 2),10);

		

		JavaPairRDD<String, PortableDataStream> rdd2 = context
				.binaryFiles("/dem3_raw/N00E033.hgt");
		
		JavaRDD<List<PointData>> rddHgtData = rdd2.map((tuple) -> hgtConvertToClass(
				tuple._1, tuple._2));
	
		
		List<PointData> initPoints = ArrayList<PointData>(0);
		
		JavaRDD<List<PointData>> rddAllPoints = rddHgtData.aggregate(initPoints, (l1, l2)-> l1, combOp)
		});
		
		//JavaPairRDD<Tuple2IntIntSerializer, Integer> rddPixelKey = rddHgtData.keyBy(data -> generateKey(data));
		
		JavaRDD<String> rddToString = rddHgtData.map(data -> data.toString());
		rddToString.saveAsTextFile("/user/lhing/aaaaaaaaaaaaaaaaaaaaaaa0000000000000000");
		

	}
	

	public static List<PointData> hgtConvertToClass(String filePath,
			PortableDataStream file) {
		DataInputStream data = file.open();
		
		/**
		 * TODO
		 * pass zoom in parameter
		 */
		long zoom = 1;
		
		System.out.println(">>>>>>>>>>>>>>>>> FILE PATH : " + filePath);

		String NS = filePath.substring(29, 30);
		String WE = filePath.substring(32, 33);
		
		long latitude = Integer.parseInt(filePath.substring(30, 32));
		long longitude = Integer.parseInt(filePath.substring(33, 36));
		
		if(NS.equals("N"))
			latitude = 90 - latitude;
		else
			latitude += 90;
		
		if(WE.equals("W"))
			longitude = 180 - longitude;
		else
			longitude += 180;

		List<PointData> pointsList = new ArrayList<PointData>();
		try {
			
			// Pixel's coordinates
			Tuple2IntIntSerializer key;
			
			char[] buf = new char[2];

			for (int i = 0; i < 1201; i++) {
				for (int j = 0; j < 1201; j++) {
					buf[0] = data.readChar();
					buf[1] = data.readChar();
					pointsList.add(new PointData(latitude, longitude, j, i, zoom, (buf[0] << 8) | buf[1]));
					
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

		return pointsList;
	}
	
	
}
