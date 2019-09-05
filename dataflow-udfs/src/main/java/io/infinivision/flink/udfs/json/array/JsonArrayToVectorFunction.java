package io.infinivision.flink.udfs.json.array;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import org.apache.flink.table.api.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;

public class JsonArrayToVectorFunction extends ScalarFunction {
	private final static Logger LOG = LoggerFactory.getLogger(JsonArrayToVectorFunction.class);


	private static final long serialVersionUID = 6632978887233948001L;

	public String eval(Object... args) {
		try {
			if (args[0] == null || args[1] == null) {
				return null;
			}
			JSONArray candidate;
			if (args[0] instanceof JSONArray) {
				candidate = (JSONArray) args[0];
			} else {
				candidate = JSON.parseArray((String) args[0]);
			}
			ArrayList<Integer> result = new ArrayList<>(candidate.size());

			// json case
			if (args[1] instanceof JSONArray || args[1].toString().trim().startsWith("[")) {
				JSONArray set;
				if (args[1] instanceof JSONArray) {
					set = (JSONArray) args[1];
				} else {
					set = JSON.parseArray((String) args[1]);
				}
				for (int i = 0; i < candidate.size(); i++) {
					if (set.contains(candidate.get(i))) {
						result.add(1);
					} else {
						result.add(0);
					}
				}
				return JSON.toJSONString(result);
			} else {
				// single string case
				for (int i = 0; i < candidate.size(); i++) {
					if (((String) args[1]).contains((String) candidate.get(i))) {
						result.add(1);
					} else {
						result.add(0);
					}
				}
				return JSON.toJSONString(result);
			}
		} catch (Throwable t) {
			LOG.error(String.format("process element error arg[0]=%s arg[1]=%s", args[0], args[1]));
			throw new RuntimeException(String.format("process element error arg[0]=%s arg[1]=%s", args[0], args[1]) + t.getMessage());
		}
	}

	public static void main(String[] args) {
		Object[] integers = Arrays.asList(3, 2, 5, 6, 2, 8, 9).toArray();
		Arrays.sort(integers, (o1, o2) -> (Integer) o2 - (Integer) o1);
		for (Object integer : integers) {
			System.out.println(integer);
		}
		System.out.println(UUID.randomUUID().toString());
		new JsonArrayToVectorFunction().eval("[500567, 500566, 1440, 500698]", "[3469,3470,3471,3472,3473,3474,1695,1715,504539,6103,6104,6105,504639,504640,504641,504642,504643,501376,4520,6291,502370,502371,502372,502373,502622,502623,502949,502952,502959,503078,503205,503672,501301,501302,503998,504214,504215,501301,504084,504085,6732,6733,5786,7058,6364,6365,502624,502217,503686,503687,504644,502759,502760,502761,502762,502763,502764,502765,502766,502767,504290,504291,504292,504293,504294,504295,504296,504297,504298]");

	}
}
