package io.infinivision.flink.udfs.json.array;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.google.common.collect.Lists;
import org.apache.flink.table.api.functions.ScalarFunction;

import java.math.BigDecimal;
import java.util.stream.IntStream;

public class JsonArrayIndexFunction extends ScalarFunction {

    private static final int ARRAY_IDX = 0;
    private static final int VALUE_IDX = 1;

    public int eval(Object... values) {

        final int[] result = {-1};

        if (null == values || values.length != 2) {
            throw new RuntimeException("arg length must 2!");
        }

        if (null == values[ARRAY_IDX] || null == values[VALUE_IDX]) {
            return result[0];
        }

        String data = (String) values[ARRAY_IDX];
        JSONArray array = JSON.parseArray(data);

        switch (values[VALUE_IDX].getClass().getName()) {
            case "java.lang.String":
                result[0] = array.indexOf(values[VALUE_IDX]);
                break;
            case "java.lang.Integer":
                result[0] = array.indexOf(values[VALUE_IDX]);
                break;
            case "java.lang.Float":
            case "java.lang.Double":
                BigDecimal value = new BigDecimal(String.valueOf(values[VALUE_IDX]));

                IntStream.range(0, array.size()).forEach(i -> {
                    if (((BigDecimal) array.get(i)).compareTo(value) == 0) {
                        result[0] = i;
                    }
                });
                break;
        }

        return result[0];
    }

    public static void main(String[] args) {
        JsonArrayIndexFunction contains = new JsonArrayIndexFunction();

        System.err.println(contains.eval("[1.90000,2.3123455631234,3.31234556312345]", 11.90000));
        System.err.println(contains.eval("[1.90000,2.3123455631234,3.31234556312345]", 2.3123455631234));
        System.err.println(contains.eval("[\"1\",\"2\",\"3\"]", 1));
        System.err.println(contains.eval("[\"1\",\"2\",\"3\"]", "1"));

        System.err.println(contains.eval(null, "1"));
        System.err.println(contains.eval("[\"1\",\"2\",\"3\"]", null));
    }

}
