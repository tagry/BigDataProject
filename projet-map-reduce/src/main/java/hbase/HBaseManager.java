package hbase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Manage the Hbase storage
 * 
 * @author maegrondin,lhing,tagry
 */
public class HBaseManager {
	private static Configuration conf = null;
	/**
	 * Initialization
	 */
	static {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "10.7.2.146");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
	}

	private final int MAX_ZOOM = 5;

	private final String tableName;

	public HBaseManager(String tableName) {
		this.tableName = tableName;
		initHBase();
	}

	/**
	 * Store map in Hbase table
	 * row->image position ex: 1-3
	 * family->zoom
	 * @param map
	 * @param zoom family in final Hbase data stored
	 */
	public void storeMap(Map<String, Map<String, Long>> map, long zoom) {
		int nbImagePerSide = (int) Math.pow(2, zoom);

		System.out.println("<<<<<<<<<<<<< START STORAGE >>>>>>>>>>>>>>>>>");
		for (int i = 0; i < nbImagePerSide; i++)
			for (int j = 0; j < nbImagePerSide; j++) {
				String key = j + "-" + i;
				if (map.containsKey(key))
					addMap(key, new String("" + zoom), map.get(key));
				else
					addMap(key, new String("" + zoom), new HashMap<String, Long>());
			}
		System.out.println("<<<<<<<<<<<<< STORAGE DONE >>>>>>>>>>>>>>>>>");

	}

	/**
	 * @param tableName
	 * @param familys
	 * @throws Exception
	 */
	private void createTable(String tableName, String[] familys) throws Exception {

		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tableName)) {
			System.out.println("<<<<<<<<<<<<<<<<<<< TABLE ALREADY EXIST" + tableName + ">>>>>>>>>>>>>");
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(tableName);
			for (int i = 0; i < familys.length; i++) {
				tableDesc.addFamily(new HColumnDescriptor(familys[i]));
			}
			admin.createTable(tableDesc);
			System.out.println("<<<<<<<<<<<<<<<< CREATE TABLE " + tableName + ">>>>>>>>>>>>>");
		}
	}


	/**
	 * @param coordImageRow image position in all images
	 * @param zoomFamily
	 * @param imageValue
	 * @throws IOException
	 */
	private void addFileToHBase(String coordImageRow, String zoomFamily, byte[] imageValue) throws IOException {
		HTable table = new HTable(conf, tableName);

		Put put = new Put(Bytes.toBytes(coordImageRow));
		put.add(Bytes.toBytes(zoomFamily), Bytes.toBytes(""), imageValue);

		table.put(put);
	}

	/**
	 * @param coordFileRow
	 * @param zoomFamily
	 * @param map
	 */
	private void addMap(String coordFileRow, String zoomFamily, Map<String, Long> map) {

		byte[] imageArray = ImageFactory.mapToArrayBytes(map);

		try {
			addFileToHBase(coordFileRow, zoomFamily, imageArray);
		} catch (Exception e) {
			System.err.println("<<<<<<<<<<<<<<<<< ERREUR ADD_MAP TABLE : " + tableName + " COORD FILE ROW : "
					+ coordFileRow + ">>>>>>>>>>>>>>>>");
			e.printStackTrace();
		}
	}

	/**
	 * 
	 */
	private void initHBase() {
		try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			if (!admin.tableExists(tableName))
				createTable(tableName, generateFamilies(MAX_ZOOM));

		} catch (Exception e) {
			System.err.println("<<<<<<<<<<<<<<<<< ERREUR INIT TABLE : " + tableName + ">>>>>>>>>>>>>>>>");
			e.printStackTrace();
		}
	}

	/**
	 * @param maxZoom
	 * @return
	 */
	private static String[] generateFamilies(int maxZoom) {
		String[] result = new String[maxZoom];

		for (int i = 0; i < maxZoom; i++)
			result[i] = i + "";

		return result;
	}
}
