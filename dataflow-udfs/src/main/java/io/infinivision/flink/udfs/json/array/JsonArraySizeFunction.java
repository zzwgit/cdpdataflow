package io.infinivision.flink.udfs.json.array;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.google.common.collect.Lists;
import org.apache.flink.table.api.functions.ScalarFunction;

import java.util.List;

public class JsonArraySizeFunction extends ScalarFunction {

    private static final int ARRAY_IDX = 0;

    public int eval(Object... values) {

        int result = 0;

        if (null == values) {
            return result;
        }

        if (values.length != 1) {
            throw new RuntimeException("arg length must 1!");
        }

        if (null == values[ARRAY_IDX]) {
            return result;
        }
        JSONArray array = Utils.getOrParse(values[0]);

        return array.size();

    }

    public static void main(String[] args) {
        JSONArray a1 = new JSONArray();
        a1.add("aaa");
        System.out.println(a1.contains(""));
        JsonArraySizeFunction contains = new JsonArraySizeFunction();

//        System.err.println(contains.eval("[1.90000,2.3123455631234,3.31234556312345]"));
//        System.err.println(contains.eval("[1,2,3]"));
//        System.err.println(contains.eval("[\"1\",\"2\",\"3\"]"));
        System.err.println(contains.eval(null));
    }
}
