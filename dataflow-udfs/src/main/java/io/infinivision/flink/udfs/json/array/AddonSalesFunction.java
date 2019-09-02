package io.infinivision.flink.udfs.json.array;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import org.apache.flink.table.api.functions.ScalarFunction;

import java.math.BigDecimal;

public class AddonSalesFunction extends ScalarFunction {

	public BigDecimal eval(Object... args) {
		BigDecimal grossSales = (BigDecimal) args[0];
		JSONArray items;
		if (args[1] instanceof JSONArray) {
			items = (JSONArray) args[1];
		} else {
			items = JSON.parseArray((String) args[1]);
		}
		JSONArray prices;
		if (args[2] instanceof JSONArray) {
			prices = (JSONArray) args[2];
		} else {
			prices = JSON.parseArray((String) args[2]);
		}
		JSONArray qty;
		if (args[3] instanceof JSONArray) {
			qty = (JSONArray) args[3];
		} else {
			qty = JSON.parseArray((String) args[3]);
		}
		JSONArray excludeItems;
		if (args[4] instanceof JSONArray) {
			excludeItems = (JSONArray) args[4];
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
