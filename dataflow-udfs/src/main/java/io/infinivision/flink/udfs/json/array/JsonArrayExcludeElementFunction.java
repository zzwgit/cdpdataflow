package io.infinivision.flink.udfs.json.array;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import org.apache.flink.table.api.functions.ScalarFunction;

public class JsonArrayExcludeElementFunction extends ScalarFunction {

	public int eval(Object... values) {
		if (values.length < 2) {
			throw new RuntimeException("args must be greater than 1");
		}

		if (values[0] == null) {
			return 0;
		}

		JSONArray base = JSON.parseArray((String) values[0]);
		for (int i = 1; i < values.length; i++) {
			if (values[i] == null) {
				continue;
			}

			if (values[i].toString().trim().startsWith("[")) {
				JSONArray exclude = JSON.parseArray((String) values[i]);
				base.removeAll(exclude);
			} else {
				base.remove(values[i]);
			}
		}
		return base.size();
	}
}
