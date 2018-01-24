/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author tagry
 */
public class Matrice {

	private static final int SIZE_BLOCK_SIDE = 1024;

	private int coordXFile;
	private int coordYFile;

	private long[][] matrice = new long[SIZE_BLOCK_SIDE][SIZE_BLOCK_SIDE];

	public Matrice(Map<String, Long> mapFile, int coordXFile, int coordYFile) {

		for (int i = 0; i < SIZE_BLOCK_SIDE; i++)
			for (int j = 0; j < SIZE_BLOCK_SIDE; j++) {
				String key = j + "-" + i;

				if (mapFile.containsKey(key))
					matrice[j][i] = mapFile.get(key);
				else
					matrice[j][i] = 0;
			}

		this.coordXFile = coordXFile;
		this.coordYFile = coordYFile;
	}

	public Matrice(byte[] byteTab, int coordXFile, int coordYFile) {
		this.coordXFile = coordXFile;
		this.coordYFile = coordYFile;

		String s = Bytes.toString(byteTab);

		int x = 0;
		int y = 0;
		String high = "";
		int c = 0;
		char charRead;

		for (int i = 0; i < s.length(); i++) {
			charRead = (char) s.charAt(i);
			switch (charRead) {
			case ',':
				matrice[y][x] = Integer.parseInt(high);
				high = "";
				x++;
				break;
			case '\n':
				c++;
				matrice[y][x] = Integer.parseInt(high);
				y++;
				x = 0;
				high = "";
				break;
			default:
				high += charRead;
				break;
			}
		}
	}

	public long getHigh(int x, int y) {
		return matrice[y][x];
	}

	public byte[] getHighBytes() {
		String s = "";
		System.out.println("add Map " + " to table " + " START6666.");

		for (int i = 0; i < SIZE_BLOCK_SIDE; i++) {
			for (int j = 0; j < SIZE_BLOCK_SIDE; j++) {
				s += matrice[i][j];
				System.out.println("blabla");
				if (j != SIZE_BLOCK_SIDE - 1)
					s += ",";
			}
			s += "\n";
		}
		System.out.println("add Map " + " to table " + " START7777.");

		return Bytes.toBytes(s);
	}

	public int getCoordXFile() {
		return coordXFile;
	}

	public int getCoordYFile() {
		return coordYFile;
	}
}
