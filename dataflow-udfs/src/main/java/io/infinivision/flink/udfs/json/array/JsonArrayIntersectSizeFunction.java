package io.infinivision.flink.udfs.json.array;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.google.common.collect.Lists;
import org.apache.flink.table.api.functions.ScalarFunction;

import java.util.List;

public class JsonArrayIntersectSizeFunction extends ScalarFunction {

	private static final int ARRAY_IDX_LEFT = 0;
	private static final int ARRAY_IDX_RIGHT = 1;

	public int eval(Object... values) {

		if (null == values || values.length != 2) {
			throw new RuntimeException("arg length must 2!");
		}

		if (null == values[ARRAY_IDX_LEFT] || null == values[ARRAY_IDX_RIGHT]) {
			return 0;
		}
		JSONArray base;
		if (values[0] instanceof String) {
			base = JSON.parseArray((String) values[0]);
		} else if (values[0] instanceof JSONArray) {
			base = (JSONArray) values[0];
		} else {
			throw new RuntimeException("unknown type " + values[0]);
		}

		JSONArray exclude;
		if (values[0] instanceof String) {
			exclude = JSON.parseArray((String) values[1]);
		} else if (values[1] instanceof JSONArray) {
			exclude = (JSONArray) values[1];
		} else {
			throw new RuntimeException("unknown type " + values[1]);
		}

		List<Object> list1 = Lists.newArrayList(base);
		List<Object> list2 = Lists.newArrayList(exclude);
		list1.retainAll(list2);
		return list1.size();
	}
}
