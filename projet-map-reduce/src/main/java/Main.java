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

import scala.reflect.internal.Trees.Return;

public class Main {

	public static void main(String[] args) {

		

		SparkConf conf = new SparkConf().setAppName("TP Spark");
		JavaSparkContext context = new JavaSparkContext(conf);

		// JavaRDD<Integer> rdd;
		// rdd = context.parallelize(Arrays.asList(1, 2, 8, 7, 2),10);

		

		JavaPairRDD<String, PortableDataStream> rdd2 = context
				.binaryFiles("/dem3_raw/N00E033.hgt");
		
		JavaRDD<HgtData> rddKey = rdd2.map((tuple) -> hgtConvertToClass(
				tuple._1, tuple._2));
	
		
		JavaRDD<String> rddToString = rddKey.map(data -> data.toString());
		rddToString.saveAsTextFile("/user/maegrondin/aaaaaaaaaaaaaaaaaaaaaaa0000000000000000");
		

	}

	public static int[][] hgtConvert(String filePath) {
		filePath = filePath.substring(5);
		File file = new File(filePath);
		int[][] height = new int[1201][1201];
		try {
			FileReader fileReader = new FileReader(file);

			char[] buf = new char[2];

			for (int i = 0; i < 1201; i++) {
				for (int j = 0; j < 1201; j++) {
					fileReader.read(buf);
					height[i][j] = (buf[0] << 8) | buf[1];
					System.out.print(height[i][j] + " ");
				}
				System.out.println("");
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return height;
	}

	public static HgtData hgtConvertToClass(String filePath,
			PortableDataStream file) {
		DataInputStream data = file.open();
		
		System.out.println(">>>>>>>>>>>>>>>>> FILE PATH : " + filePath);

		int latitude = Integer.parseInt(filePath.substring(30, 32));
		int longitude = Integer.parseInt(filePath.substring(33, 36));

		int[][] height = new int[1201][1201];
		try {
			char[] buf = new char[2];

			for (int i = 0; i < 1201; i++) {
				for (int j = 0; j < 1201; j++) {
					buf[0] = data.readChar();
					buf[1] = data.readChar();
					height[i][j] = (buf[0] << 8) | buf[1];
				}
				data.close();
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return new HgtData(latitude, longitude, height);
	}
}
