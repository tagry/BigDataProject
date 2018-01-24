package mapprinter;
import java.awt.Color;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.awt.image.WritableRaster;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;

import javax.imageio.ImageIO;

public class ImageManager {
	private static final int IMAGE_SIZE = 1024;

	public static BufferedImage mapToImage(Map<String, Long> map) {
		BufferedImage image = new BufferedImage(IMAGE_SIZE, IMAGE_SIZE, BufferedImage.TYPE_BYTE_BINARY);

		for (int i = 0; i < IMAGE_SIZE; i++)
			for (int j = 0; j < IMAGE_SIZE; j++) {
				String key = j + "-" + i;

				if (map.containsKey(key))
					image.setRGB(j, i, getRgbColorFromAltitude(map.get(key)));
				else
					image.setRGB(j, i, Color.WHITE.getRGB());
			}

		return image;

	}

	public static byte[] imageToArrayBytes(BufferedImage image) {
		// get DataBufferBytes from Raster
		WritableRaster raster = image.getRaster();
		DataBufferByte data = (DataBufferByte) raster.getDataBuffer();

		return (data.getData());
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
