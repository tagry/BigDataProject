package hbase;
import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import javax.imageio.ImageIO;

/**
 * Image factory to array byte data or BufferedImage
 * @author maegrondin,lhing,tagry
 *
 */
public class ImageFactory {
	private static final int IMAGE_SIZE = 1024;
	
	
	/**
	 * Get the image in array bytes format from map
	 * @param map
	 * @return image in array bytes format
	 */
	public static byte[] mapToArrayBytes(Map<String, Long> map) {
		return imageToArrayBytes(mapToImage(map));
	}

	/**
	 * Get the BufferedImage from map
	 * @param map
	 * @return Image
	 */
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

	/**
	 * @param image
	 * @return
	 */
	private static byte[] imageToArrayBytes(BufferedImage image) {
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
	

	/**
	 * Choice the color from the altitude
	 * @param altitude
	 * @return pixel color
	 */
	private static int getRgbColorFromAltitude(long altitude) {
		int rgbColor;

		if (altitude <= 0)
			rgbColor = Color.BLUE.getRGB();
		else if(altitude <= 150)
			rgbColor = new Color(76,129,41,255).getRGB();
		else if(altitude <= 300)
			rgbColor = new Color(193,201,80,255).getRGB();
		else if(altitude <= 500)
			rgbColor = new Color(234,191,76,255).getRGB();
		else if(altitude <= 1000)
			rgbColor = new Color(227,141,38,255).getRGB();
		else if(altitude <= 1500)
			rgbColor = new Color(217,94,14,255).getRGB();
		else if(altitude <= 3000)
			rgbColor = new Color(128,46,6,255).getRGB();
		else if(altitude <= 4000)
			rgbColor = new Color(192,192,192,255).getRGB();
		else
			rgbColor = Color.WHITE.getRGB();

		return rgbColor;
	}
}
