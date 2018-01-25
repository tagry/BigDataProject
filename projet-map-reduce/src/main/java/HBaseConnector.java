import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseConnector {
	private static Configuration conf = null;
	/**
	 * Initialization
	 */
	static {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "10.7.2.146");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
	}

	/**
	 * Create a table
	 */
	public static void createTable(String tableName, String[] familys) throws Exception {

		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tableName)) {
			System.out.println("table already exists!");
		} else {
			System.out.println("ok");
			HTableDescriptor tableDesc = new HTableDescriptor(tableName);
			for (int i = 0; i < familys.length; i++) {
				tableDesc.addFamily(new HColumnDescriptor(familys[i]));
			}
			System.out.println("ok");
			admin.createTable(tableDesc);
			System.out.println("create table " + tableName + " ok.");
		}
	}

	/**
	 * Delete a table
	 */
	public static void deleteTable(String tableName) throws Exception {
		try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			System.out.println("delete table " + tableName + " ok.");
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * @param tableName
	 * @param coordFileRow
	 * @param zoomFamily
	 *            x-y
	 * @param xyPixel
	 *            x-y (1024 x 1024)
	 * @param valueImage
	 * @throws Exception
	 */
	public static void addFileToHBase(String tableName, String coordFileRow, String zoomFamily, byte[] valueImage)
			throws Exception {
		try {
			HTable table = new HTable(conf, tableName);

			Put put = new Put(Bytes.toBytes(coordFileRow));

			put.add(Bytes.toBytes(zoomFamily), Bytes.toBytes(""), valueImage);

			table.put(put);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void addMap(String tableName, String coordFileRow, String zoomFamily, Map<String, Long> map) {

		BufferedImage image = ImageManager.mapToImage(map);

		try {
			addFileToHBase(tableName, coordFileRow, zoomFamily, ImageManager.imageToArrayBytes(image));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Delete a row
	 */
	public static void delZoomRow(String tableName, String zoomRow) throws IOException {
		HTable table = new HTable(conf, tableName);
		List<Delete> list = new ArrayList<Delete>();
		Delete del = new Delete(zoomRow.getBytes());

		list.add(del);
		table.delete(list);
		System.out.println("del recored " + zoomRow + " ok.");
	}

	/**
	 * Get a row
	 */
	public static Map<String, Long> getOneImage(String tableName, String coordFileRow, byte[] zoomFamily) throws IOException {
		Map<String, Long> map = new HashMap<String, Long>();
		HTable table = new HTable(conf, tableName);
		Get get = new Get(coordFileRow.getBytes());
		get.addFamily(zoomFamily);
		Result rs = table.get(get);
		for (KeyValue kv : rs.raw()) {

			Long l = new Long(4);
			String s = " ";

			map.put(new String(kv.getQualifier()), Bytes.toLong(kv.getValue()));
			System.out.print(new String(kv.getQualifier()) + " ");
			System.out.println(new String(kv.getValue()));
		}
		return map;
	}

	public static void initHBase(String tableName, String[] familys, long zoom) {
		try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			if (!admin.tableExists(tableName))
				createTable(tableName, familys);

			//delZoomRow(tableName, new String(zoom + ""));

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
