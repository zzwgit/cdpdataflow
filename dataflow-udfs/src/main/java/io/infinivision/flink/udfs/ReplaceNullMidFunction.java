package io.infinivision.flink.udfs;

import org.apache.flink.table.api.functions.ScalarFunction;

import java.util.concurrent.atomic.AtomicInteger;

public class ReplaceNullMidFunction extends ScalarFunction {
	private static final long serialVersionUID = 9212898017644650475L;
	private AtomicInteger i = new AtomicInteger(0);

	public String eval(String prefix, String mid) {
		if (mid == null || mid.length() == 0) {
			return prefix + i.getAndIncrement();
		} else {
			return mid;
		}
	}
}
