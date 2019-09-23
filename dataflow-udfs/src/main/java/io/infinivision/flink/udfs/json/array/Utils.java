package io.infinivision.flink.udfs.json.array;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class Utils {
	public static JSONArray getOrParse(Object obj) {
		if (obj instanceof String) {
			return JSONObject.parseArray(((String) obj));
		} else if (obj instanceof JSONArray) {
			return ((JSONArray) obj);
		} else {
			throw new RuntimeException("unknown type " + obj.getClass().getName());
		}
	}
}
