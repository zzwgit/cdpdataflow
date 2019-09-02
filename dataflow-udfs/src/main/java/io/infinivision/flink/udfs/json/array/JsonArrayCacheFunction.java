package io.infinivision.flink.udfs.json.array;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import org.apache.flink.table.api.functions.ScalarFunction;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 目的: 缓存一条记录处理流程里的某些字段需要被多次Parse的情况
 * 这样只需要json.parse一次
 */
public class JsonArrayCacheFunction extends ScalarFunction {

	static class Element {
		String prevKey = "";
		JSONArray prevArray;

		public Element(String prevKey, JSONArray prevArray) {
			this.prevKey = prevKey;
			this.prevArray = prevArray;
		}
	}

	// namespace --> element
	private ConcurrentHashMap<String, Element> cache
			= new ConcurrentHashMap<>();

	public JSONArray eval(String nameSpace, String key, String value) {
		if (!cache.containsKey(nameSpace) || !cache.get(nameSpace).prevKey.equals(key)) {
			cache.put(nameSpace, new Element(key, JSON.parseArray(value)));
		}
		return cache.get(nameSpace).prevArray;
	}
}
