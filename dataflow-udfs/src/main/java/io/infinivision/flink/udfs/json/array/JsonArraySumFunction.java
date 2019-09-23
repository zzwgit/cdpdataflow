package io.infinivision.flink.udfs.json.array;

import com.alibaba.fastjson.JSONArray;
import org.apache.flink.table.api.functions.ScalarFunction;

public class JsonArraySumFunction extends ScalarFunction {
	public double eval(Object arg) {
		int result = 0;
		if (arg == null) {
			return result;
		}
		JSONArray array = Utils.getOrParse(arg);
		for (Object o : array) {
			if (o instanceof Integer) {
				result += ((Integer) o);
			} else if (o instanceof Double) {
				result += ((Double) o);
			}
		}
		return result;
	}
}
