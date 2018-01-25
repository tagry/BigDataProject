import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import javax.imageio.ImageIO;

public class ImageManager {
	private static final int IMAGE_SIZE = 1024;

	public static BufferedImage mapToImage(Map<String, Long> map) {
		BufferedImage image = new BufferedImage(IMAGE_SIZE, IMAGE_SIZE,
				BufferedImage.TYPE_INT_RGB);

		for (int i = 0; i < IMAGE_SIZE; i++)
			for (int j = 0; j < IMAGE_SIZE; j++) {
				String key = j + "-" + i;

				if (map.containsKey(key))
					image.setRGB(j, i, getRgbColorFromAltitude(map.get(key)));
				else
					image.setRGB(j, i, Color.BLUE.getRGB());
			}

		return image;

	}
	

	public static byte[] imageToArrayBytes(BufferedImage image) {
		// get DataBufferBytes from Raster
		// WritableRaster raster = image.getRaster();
		// DataBufferByte data = (DataBufferByte) raster.getDataBuffer();

		ByteArrayOutputStream baos = new ByteArrayOutputStream(IMAGE_SIZE*IMAGE_SIZE);
		byte[] imgBytes = null;
		
		try {
			ImageIO.write(image, "jpg", baos);
			baos.flush();
			
			imgBytes = baos.toByteArray();
			baos.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return imgBytes;
	}
	

	public static BufferedImage arrayByteToImage(byte[] imageData) {
		ByteArrayInputStream bais = new ByteArrayInputStream(imageData);
		try {
			return ImageIO.read(bais);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	

	private static int getRgbColorFromAltitude(long altitude) {
		int rgbColor;

		if (altitude <= 0)
			rgbColor = Color.BLUE.getRGB();
		else if (altitude < 2000)
			rgbColor = Color.GREEN.getRGB();
		else if (altitude < 3000)
			rgbColor = Color.YELLOW.getRGB();
		else
			rgbColor = Color.WHITE.getRGB();

		return rgbColor;
	}
}
