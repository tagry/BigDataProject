
public class PointData {	
	private long coordPixelX;
	private long coordPixelY;
	
	private long altitude;
	
	public PointData(long latitude, long longitude,long coordX, long coordY,long zoom,  long altitude) {
		this.altitude = altitude;
		setCoordsPixel(latitude, longitude, coordX, coordY, zoom);
	}
	
	private void setCoordsPixel(long latitude, long longitude,long coordX, long coordY,long zoom){
		long step = 180 * 1201 / (zoom * 1024);
		this.coordPixelX = (longitude * 1201 + coordX) / (step * 2);
		this.coordPixelY = ((latitude - 1) * 1201 + coordY) / step;
	}
}
