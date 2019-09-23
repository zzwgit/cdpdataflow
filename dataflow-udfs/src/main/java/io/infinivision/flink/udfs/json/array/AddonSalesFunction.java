package io.infinivision.flink.udfs.json.array;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import org.apache.flink.table.api.functions.ScalarFunction;

import java.math.BigDecimal;

public class AddonSalesFunction extends ScalarFunction {

	public BigDecimal eval(Object... args) {
		BigDecimal grossSales = (BigDecimal) args[0];
		JSONArray items = Utils.getOrParse(args[1]);
		JSONArray prices = Utils.getOrParse(args[2]);
		JSONArray qty = Utils.getOrParse(args[3]);
		JSONArray excludeItems;
		if (args[4] instanceof JSONArray) {
			excludeItems = (JSONArray) args[4];
		} else if (!((String) args[4]).startsWith("[")) {
			excludeItems = new JSONArray();
			excludeItems.add(args[4]);
		} else {
			excludeItems = JSON.parseArray((String) args[4]);
		}

		BigDecimal result = new BigDecimal(0.00);

		for (int i = 0; i < items.size(); i++) {
			if (!excludeItems.contains(items.get(i))) {
				result = result.add(
						new BigDecimal(qty.get(i).toString())
								.multiply(
										new BigDecimal(prices.get(i).toString())
								));
			}
		}
		return result;
	}
}
