package io.infinivision.flink.udfs.json.array;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import org.apache.flink.table.api.functions.ScalarFunction;

import java.math.BigDecimal;

public class JsonArrayContainsFunction extends ScalarFunction {

    private static final int ARRAY_IDX = 0;
    private static final int VALUE_IDX = 1;

    public Boolean eval(Object... values) {

        Boolean result = false;

        if (null == values || values.length != 2) {
            throw new RuntimeException("arg length must 2!");
        }

        if (null == values[ARRAY_IDX] || null == values[VALUE_IDX]) {
            return result;
        }

        String data = (String) values[ARRAY_IDX];
        JSONArray array = JSON.parseArray(data);

        switch (values[VALUE_IDX].getClass().getName()) {
            case "java.lang.String":
                if (array.contains(values[VALUE_IDX])) {
                    result = true;
                }
                break;
            case "java.lang.Integer":
                if (array.contains(values[VALUE_IDX])) {
                    result = true;
                }
                break;
            case "java.lang.Float":
            case "java.lang.Double":
                BigDecimal value = new BigDecimal(String.valueOf(values[VALUE_IDX]));
                result = array.stream().anyMatch(i -> ((BigDecimal) i).compareTo(value) == 0);
                break;
        }

        return result;
    }

    public static void main(String[] args) {
        JsonArrayContainsFunction contains = new JsonArrayContainsFunction();

        System.err.println(contains.eval("[1.90000,2.312345563123455631234556,3.312345563123455631234556]", "1.9"));
        System.err.println(contains.eval("[1.90000,2.312345563123455631234556,3.312345563123455631234556]", 1.9));
        System.err.println(contains.eval("[\"1\",\"2\",\"3\"]", "1"));
        System.err.println(contains.eval("[\"1\",\"2\",\"3\"]", 1));
        System.err.println(contains.eval("[1,2,3]", "1"));
        System.err.println(contains.eval("[1,2,3]", 1));

        System.err.println(contains.eval(null, 1));
        System.err.println(contains.eval("[1,2,3]", null));
    }

}
