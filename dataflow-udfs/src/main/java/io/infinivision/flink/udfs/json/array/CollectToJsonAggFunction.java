package io.infinivision.flink.udfs.json.array;

import com.alibaba.fastjson.JSON;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.table.api.dataview.ListView;
import org.apache.flink.table.api.functions.AggregateFunction;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.dataformat.GenericRow;

import java.util.Iterator;
import java.util.List;

public class CollectToJsonAggFunction extends AggregateFunction<byte[], GenericRow> {

    @Override
    public GenericRow createAccumulator() {
        GenericRow acc = new GenericRow(2);
        // adding list
        acc.update(0, new ListView<Object>());
        // retractList
        acc.update(1, new ListView<Object>());

        return acc;
    }

    public void accumulate(GenericRow acc, Object input) throws Exception {
        if (null != input) {
            ListView<Object> list = (ListView<Object>) acc.getField(0);
            list.add(input);
        }
    }

    public void retract(GenericRow acc, Object input) throws Exception {
        if (null != input) {
            ListView<Object> list = (ListView<Object>) acc.getField(0);
            if (!list.remove(input)) {
                ListView<Object> retractList = (ListView<Object>) acc.getField(1);
                retractList.add(input);
            }
        }
    }

    public void merge(GenericRow acc, Iterable<GenericRow> it) throws Exception {
        Iterator<GenericRow> iter = it.iterator();
        while (iter.hasNext()) {
            GenericRow otherAcc = iter.next();
            ListView<Object> thisList = (ListView<Object>) acc.getField(0);
            ListView<Object> otherList = (ListView<Object>) otherAcc.getField(0);
            Iterable<Object> accList = otherList.get();
            if (accList != null) {
                Iterator<Object> listIter = accList.iterator();
                while (listIter.hasNext()) {
                    thisList.add(listIter.next());
                }
            }

            ListView<Object> otherRetractList = (ListView<Object>) otherAcc.getField(1);
            ListView<Object> thisRetractList = (ListView<Object>) acc.getField(1);
            Iterable<Object> retractList = otherRetractList.get();
            if (retractList != null) {
                Iterator<Object> retractListIter = retractList.iterator();
                List<Object> buffer = null;
                if (retractListIter.hasNext()) {
                    buffer = Lists.newArrayList(thisList.get());
                }
                boolean listChanged = false;
                while (retractListIter.hasNext()) {
                    Object element = retractListIter.next();
                    if (buffer != null && buffer.remove(element)) {
                        listChanged = true;
                    } else {
                        thisRetractList.add(element);
                    }
                }

                if (listChanged) {
                    thisList.clear();
                    thisList.addAll(buffer);
                }
            }
        }
    }

    @Override
    public byte[] getValue(GenericRow acc) {
        ListView<Object> list = (ListView<Object>) acc.getField(0);
        try {
            Iterable<Object> values = list.get();
            if (values == null || !values.iterator().hasNext()) {
                // return null when the list is empty
                return null;
            } else {
                //values.
                Object[] out = Lists.newArrayList(values).toArray();
                String outStr = JSON.toJSONString(out);
                return outStr.getBytes("UTF-8");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public void resetAccumulator(GenericRow acc) {
        ListView<Object> list = (ListView<Object>) acc.getField(0);
        ListView<Object> retractList = (ListView<Object>) acc.getField(1);
        list.clear();
        retractList.clear();
    }

    @Override
    public DataType getResultType() {
        return DataTypes.BYTE_ARRAY;
    }

    public static void main(String[] args) throws Exception {
        CollectToJsonAggFunction s = new CollectToJsonAggFunction();

        GenericRow acc = new GenericRow(2);
        // adding list
        acc.update(0, new ListView<Object>());

        s.accumulate(acc, "1");
        s.accumulate(acc, "1");

        byte[] result = s.getValue(acc);
        System.out.println(new String(result, "UTF-8"));

        String ss = "[\"1\",\"1\"]";

//        byte[] result1 = new byte[]{91, 49, 52, 54, 52, 49, 49, 54, 44, 49, 52, 54, 52, 49, 49, 54, 44, 49, 52, 54, 52, 49, 49, 54, 93};
//        byte[] result2 = new byte[]{91, 52, 51, 51, 57, 55, 50, 50, 93};
//        System.out.println(new String(result1,"UTF-8") );
//        System.out.println(new String(result2,"UTF-8") );
//
//        JSONArray array =JSON.parseArray(new String(result1,"UTF-8"));
//        array.toArray();

    }
}
