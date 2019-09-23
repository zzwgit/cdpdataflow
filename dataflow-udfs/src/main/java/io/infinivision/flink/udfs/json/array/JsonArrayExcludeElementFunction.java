package io.infinivision.flink.udfs.json.array;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import org.apache.flink.table.api.functions.ScalarFunction;

import java.util.Collections;

public class JsonArrayExcludeElementFunction extends ScalarFunction {

	public int eval(Object... values) {
		if (values.length < 2) {
			throw new RuntimeException("args must be greater than 1");
		}

		if (values[0] == null) {
			return 0;
		}

		JSONArray base = Utils.getOrParse(values[0]);
		for (int i = 1; i < values.length; i++) {
			if (values[i] == null) {
				continue;
			}
			if (values[i] instanceof JSONArray) {
				base.removeAll((JSONArray) values[i]);
			} else if (values[i].toString().trim().startsWith("[")) {
				JSONArray exclude = JSON.parseArray((String) values[i]);
				base.removeAll(exclude);
			} else {
				base.removeAll(Collections.singletonList(values[i]));
			}
		}
		return base.size();
	}
}
