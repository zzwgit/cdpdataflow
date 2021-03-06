package io.infinivision.flink.udfs.json.array;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.google.common.collect.Lists;
import org.apache.flink.table.api.functions.ScalarFunction;

import java.util.Arrays;
import java.util.List;

public class JsonArrayIntersectFunction extends ScalarFunction {

    private static final int ARRAY_IDX_LEFT = 0;
    private static final int ARRAY_IDX_RIGHT = 1;

    public String eval(Object... values) {

        if (null == values || values.length != 2) {
            throw new RuntimeException("arg length must 2!");
        }

        if (null == values[ARRAY_IDX_LEFT] || null == values[ARRAY_IDX_RIGHT]) {
            return JSON.toJSONString(Lists.newArrayList());
        }

        List<Object> list1 = Lists.newArrayList(JSON.parseArray((String) values[ARRAY_IDX_LEFT]));
        List<Object> list2 = Lists.newArrayList(JSON.parseArray((String) values[ARRAY_IDX_RIGHT]));
        list1.retainAll(list2);
        return JSON.toJSONString(list1);

    }

    public static void main(String[] args) {
        JsonArrayIntersectFunction contains = new JsonArrayIntersectFunction();

        System.err.println(contains.eval("[1.90000,2.3123455631234,3.31234556312345]", "[1.90000,3.31234556312345]"));
        System.err.println(contains.eval("[1,2,3]", "[2,3,4]"));
        System.err.println(contains.eval("[\"1\",\"2\",\"3\"]", "[\"2\",\"3\",\"4\"]"));
        System.err.println(contains.eval(null, "[\"2\",\"3\",\"4\"]"));
    }
}
