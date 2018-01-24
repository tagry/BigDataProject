package mapprinter;

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

import scala.reflect.api.Trees.IfApi;

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
	 * @param zoomRow
	 * @param fileFamily
	 *            x-y
	 * @param xyPixel
	 *            x-y (1024 x 1024)
	 * @param value
	 * @throws Exception
	 */
	public static void addPixel(String tableName, String zoomRow, String fileFamily, String xyPixel, String value)
			throws Exception {
		try {
			HTable table = new HTable(conf, tableName);
			Put put = new Put(Bytes.toBytes(zoomRow));
			put.add(Bytes.toBytes(fileFamily), Bytes.toBytes(xyPixel), Bytes.toBytes(value));
			table.put(put);
			System.out.println("insert pixel " + zoomRow + " to table " + tableName + " ok.");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void addMap(String tableName, String zoomRow, String fileFamily, Map<String, Long> map) {
		map.forEach((k, v) -> {
			try {
				addPixel(tableName, zoomRow, fileFamily, k, v.toString());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		});
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
	public static BufferedImage getOneImage(String tableName, String coordRow, byte[] zoomFamily) throws IOException {

		HTable table = new HTable(conf, tableName);
		Get get = new Get(coordRow.getBytes());
		Result rs = table.get(get);

		BufferedImage image = ImageManager.arrayByteToImage(rs.getValue(zoomFamily, "".getBytes()));
		
		return image;
	}

	public static void initHBase(String tableName, String[] familys, long zoom) {
		try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			if (!admin.tableExists(tableName))
				createTable(tableName, familys);

			delZoomRow(tableName, new String(zoom + ""));

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/*
	 * public static void test() { try { String tablename = "scores"; String[]
	 * familys = { "grade", "course" }; System.out.println("ok");
	 * HBaseConnector.createTable(tablename, familys); System.out.println("ok"); //
	 * add record zkb System.out.println("ok"); HBaseConnector.addPixel(tablename,
	 * "zkb", "grade", "", "5"); HBaseConnector.addPixel(tablename, "zkb", "course",
	 * "", "90"); HBaseConnector.addPixel(tablename, "zkb", "course", "math", "97");
	 * HBaseConnector.addPixel(tablename, "zkb", "course", "art", "87");
	 * System.out.println("ok"); // add record baoniu
	 * HBaseConnector.addPixel(tablename, "baoniu", "grade", "", "4");
	 * HBaseConnector.addPixel(tablename, "baoniu", "course", "math", "89");
	 * 
	 * System.out.println("===========get one record========");
	 * HBaseConnector.getOneRecord(tablename, "zkb");
	 * 
	 * System.out.println("===========show all record========");
	 * HBaseConnector.getAllRecord(tablename);
	 * 
	 * System.out.println("===========del one record========");
	 * HBaseConnector.delZoomRow(tablename, "baoniu");
	 * HBaseConnector.getAllRecord(tablename);
	 * 
	 * System.out.println("===========show all record========");
	 * HBaseConnector.getAllRecord(tablename); } catch (Exception e) {
	 * e.printStackTrace(); } }
	 */
}
