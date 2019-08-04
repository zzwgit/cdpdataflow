package io.infinivision.flink.udfs.json.array;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import org.apache.flink.table.api.functions.ScalarFunction;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.stream.Collectors;

public class JsonArrayCountElementFunction extends ScalarFunction {

	private static final int ARRAY_IDX = 0;
	private static final int VALUE_IDX = 1;

	public int eval(Object... values) {
		if (null == values || values.length != 2) {
			throw new RuntimeException("arg length must 2!");
		}

		if (null == values[ARRAY_IDX] || null == values[VALUE_IDX]) {
			return 0;
		}

		String data = (String) values[ARRAY_IDX];
		JSONArray array = JSON.parseArray(data);

		switch (values[VALUE_IDX].getClass().getName()) {
			case "java.lang.String":
				return Collections.frequency(array, values[VALUE_IDX]);
			case "java.lang.Integer":
			case "java.lang.Float":
			case "java.lang.Double":
				// attention!!! BigDecimal(2.0) != BigDecimal(2)
				// so convert to double first
				return Collections.frequency(array.stream().map(e -> new BigDecimal(Double.parseDouble(e.toString()))).collect(Collectors.toList()),
						new BigDecimal(Double.parseDouble(String.valueOf(values[VALUE_IDX]))));
			default:
				return 0;
		}
	}

	public static void main(String[] args) {
		JsonArrayCountElementFunction function = new JsonArrayCountElementFunction();
		int count = function.eval("[\"23\",\"34\",\"23\"]", "23");
		System.out.println(count);
		BigDecimal bigDecimal = new BigDecimal("2");
		count = function.eval("[23.0,34,23]", 23);
		System.out.println(count);

	}
}
