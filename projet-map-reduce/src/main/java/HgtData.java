import org.apache.spark.api.java.JavaRDD;


public class HgtData {
	int latitude;
	int longitude;
	int[][] height;
	
	public HgtData(int latitude, int longitude, int[][] height){
		this.latitude = latitude;
		this.longitude = longitude;
		this.height = height;
	}
	
	public int getLatitude() {
		return latitude;
	}
	
	public void setLatitude(int latitude) {
		this.latitude = latitude;
	}
	
	public int getLongitude() {
		return longitude;
	}
	
	public void setLongitude(int longitude) {
		this.longitude = longitude;
	}
	
	public int[][] getHeight() {
		return height;
	}
	
	public void setHeight(int[][] height) {
		this.height = height;
	}
	
	@Override
	public String toString() {
		return longitude + " " + latitude;
	}
	
}
