package io.druid.indexing.kafka;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import com.metamx.emitter.EmittingLogger;

public class MongoUtils {
	
	public static Date dateFromOidString(String oidString){
		byte[] bytes = new byte[4];
		for (int i = 0; i < bytes.length; i++) {
			bytes[i] = (byte) Integer.parseInt(oidString.substring(i * 2, i * 2 + 2), 16);
		}
		int timestamp = makeInt(bytes[0], bytes[1], bytes[2], bytes[3]);
		return new Date(timestamp * 1000L);
	}
	
	public static String isoDateFromOidString(String oidString){
		TimeZone tz = TimeZone.getTimeZone("UTC");
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'"); // Quoted "Z" to indicate UTC, no timezone offset
		df.setTimeZone(tz);
		return df.format(dateFromOidString(oidString));
	}
	
	private static int makeInt(final byte b3, final byte b2, final byte b1, final byte b0) {
        return (((b3) << 24) |
                ((b2 & 0xff) << 16) |
                ((b1 & 0xff) << 8) |
                ((b0 & 0xff)));
    }
	
	private static byte[] parseHexString(final String s) {

		byte[] b = new byte[4];
		for (int i = 0; i < b.length; i++) {
			b[i] = (byte) Integer.parseInt(s.substring(i * 2, i * 2 + 2), 16);
		}
		return b;
	}
}
