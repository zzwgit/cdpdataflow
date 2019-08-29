package io.infinivision.flink.udfs.json.array;

import com.alibaba.fastjson.JSON;
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

		List<Object> list1 = Lists.newArrayList(JSON.parseArray((String) values[ARRAY_IDX_LEFT]));
		List<Object> list2 = Lists.newArrayList(JSON.parseArray((String) values[ARRAY_IDX_RIGHT]));
		list1.retainAll(list2);
		return list1.size();
	}
}
