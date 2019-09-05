package io.infinivision.flink.udfs;

import org.apache.flink.table.api.functions.ScalarFunction;

import java.text.SimpleDateFormat;
import java.util.Date;

// 目前 blink dataformat timestamp用的时区是utc ,所以需要个udf使用本地时区
public class TimestampToStr extends ScalarFunction {

	private transient static ThreadLocal<Tuple2> sdfDate;

	static class Tuple2 {
		SimpleDateFormat sdf;
		Date date;
	}

	// for gc friendly
	static {
		sdfDate = new ThreadLocal<Tuple2>() {

			@Override
			protected Tuple2 initialValue() {
				Tuple2 t = new Tuple2();
				t.sdf = new SimpleDateFormat(TIMESTAMP_FORMAT_STRING);
				t.date = new Date();
				return t;
			}
		};
	}

	public static final String DATE_FORMAT_STRING = "yyyy-MM-dd";

	/**
	 * The SimpleDateFormat string for ISO times, "HH:mm:ss".
	 */
	public static final String TIME_FORMAT_STRING = "HH:mm:ss";

	/**
	 * The SimpleDateFormat string for ISO timestamps, "yyyy-MM-dd HH:mm:ss".
	 */
	public static final String TIMESTAMP_FORMAT_STRING =
			DATE_FORMAT_STRING + " " + TIME_FORMAT_STRING;

	// timestamp本来就是个Long表示
	public String eval(long ts) {
		if (ts <= 1000) {
			return null;
		}
//		return new SimpleDateFormat(TIMESTAMP_FORMAT_STRING).format(new Date(ts));
		sdfDate.get().date.setTime(ts);
		return sdfDate.get().sdf.format(sdfDate.get().date);
	}
}
