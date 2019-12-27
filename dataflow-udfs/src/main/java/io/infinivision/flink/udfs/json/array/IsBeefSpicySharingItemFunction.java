package io.infinivision.flink.udfs.json.array;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import org.apache.flink.table.api.functions.ScalarFunction;

import java.util.*;

public class IsBeefSpicySharingItemFunction extends ScalarFunction {
	private List<String> beefSpicy = Arrays.asList("beef", "spicy", "non_spicy");

	public String eval(Object... args) {
		String type = (String) args[0];
		LinkedList<Integer> result = new LinkedList<>();
		JSONArray level1;
		level1 = Utils.getOrParse(args[1]);

		if (beefSpicy.contains(type)) {
			JSONArray protein = Utils.getOrParse(args[2]);
			JSONArray level8 = null;
			if (type.contains("spicy")) {
				level8 = Utils.getOrParse(args[3]);
			}
			for (int i = 0; i < level1.size(); i++) {
				String var = (String) level1.get(i);
				if (!var.contains("ALC Entrée") && !var.contains("EVM") && !var.contains("Other Meal")) {
					result.add(0);
					continue;
				}
				String var2 = protein.getString(i);
				if (type.contains("beef")) {
					result.add(var2.toLowerCase().contains("beef") ? 1 : 0);
				} else {
					String var3 = level8.getString(i);
					boolean chicken = var2.toLowerCase().contains("chicken");
					boolean spicy = var3.toLowerCase().contains("spicy");
					if (type.contains("non")) {
						result.add(chicken && !spicy ? 1 : 0);
					} else {
						result.add(chicken && spicy ? 1 : 0);
					}
				}
			}
		} else {
			JSONArray level3 = Utils.getOrParse(args[2]);
			JSONArray qty;
			qty = Utils.getOrParse(args[3]);
			if (level1.size() < 5) {
				for (int i = 0; i < level1.size(); i++) {
					result.add(0);
				}
			} else {
				int burgerCnt = 0;
				for (int i = 0; i < level1.size(); i++) {
					String var2 = level3.getString(i);
					Integer var3 = qty.getInteger(i);
					if (var2.toLowerCase().contains("burger") && var3 != null) {
						burgerCnt += var3;
					}
				}
				for (int i = 0; i < level1.size(); i++) {
					String var1 = level1.getString(i);
					String var2 = level3.getString(i);
					Integer var3 = qty.getInteger(i);
					if (var1.contains("Happy Meal") || var2.toLowerCase().contains("burger") && burgerCnt > 1) {
						result.add(1);
					} else {
						result.add(0);
					}
				}
			}
		}
		return JSON.toJSONString(result);
	}
}
