package io.infinivision.flink.udfs.json.array;

import com.alibaba.fastjson.JSON;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.table.api.dataview.ListView;
import org.apache.flink.table.api.functions.AggregateFunction;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.dataformat.GenericRow;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class CollectToJsonAggFunction extends AggregateFunction<String, GenericRow> {
	// 封装 数据 和 排序参考key
	static class Tuple2 implements Serializable {
		private static final long serialVersionUID = 4827706635758733641L;
		Object data; // 实际数据
		List<Comparable> orderKeys;  // 排序参考key

		Tuple2(Object data) {
			this.data = data;
			this.orderKeys = Collections.emptyList();
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			Tuple2 tuple2 = (Tuple2) o;
			return Objects.equals(data, tuple2.data) &&
					Objects.equals(orderKeys, tuple2.orderKeys);
		}

		@Override
		public int hashCode() {
			return Objects.hash(data);
		}
	}


	@Override
	public GenericRow createAccumulator() {
		GenericRow acc = new GenericRow(2);
		// adding list
		acc.update(0, new ListView<Tuple2>());
		// retractList
		acc.update(1, new ListView<Tuple2>());

		return acc;
	}

	public void accumulate(GenericRow acc, Object data, Object... orderKeys) throws Exception {
		if (null != data) {
			ListView<Tuple2> list = (ListView<Tuple2>) acc.getField(0);
			list.add(from(data, orderKeys));
		}
	}

	private Tuple2 from(Object data, Object... orderKeys) {
		Tuple2 t = new Tuple2(data);
		if (orderKeys != null && orderKeys.length > 0) {
			t.orderKeys = new ArrayList<>(orderKeys.length);
			for (Object key : orderKeys) {
				t.orderKeys.add((Comparable) key);
			}
		}
		return t;
	}

	public void retract(GenericRow acc, Object data, Object... orderKeys) throws Exception {
		if (null != data) {
			ListView<Tuple2> list = (ListView<Tuple2>) acc.getField(0);
			Tuple2 t = from(data, orderKeys);
			if (!list.remove(t)) {
				ListView<Tuple2> retractList = (ListView<Tuple2>) acc.getField(1);
				retractList.add(t);
			}
		}
	}

	public void merge(GenericRow acc, Iterable<GenericRow> it) throws Exception {
		Iterator<GenericRow> iter = it.iterator();
		while (iter.hasNext()) {
			GenericRow otherAcc = iter.next();
			ListView<Tuple2> thisList = (ListView<Tuple2>) acc.getField(0);
			ListView<Tuple2> otherList = (ListView<Tuple2>) otherAcc.getField(0);
			Iterable<Tuple2> accList = otherList.get();
			if (accList != null) {
				Iterator<Tuple2> listIter = accList.iterator();
				while (listIter.hasNext()) {
					thisList.add(listIter.next());
				}
			}

			ListView<Tuple2> otherRetractList = (ListView<Tuple2>) otherAcc.getField(1);
			ListView<Tuple2> thisRetractList = (ListView<Tuple2>) acc.getField(1);
			Iterable<Tuple2> retractList = otherRetractList.get();
			if (retractList != null) {
				Iterator<Tuple2> retractListIter = retractList.iterator();
				List<Tuple2> buffer = null;
				if (retractListIter.hasNext()) {
					buffer = Lists.newArrayList(thisList.get());
				}
				boolean listChanged = false;
				while (retractListIter.hasNext()) {
					Tuple2 element = retractListIter.next();
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
	public String getValue(GenericRow acc) {
		ListView<Tuple2> list = (ListView<Tuple2>) acc.getField(0);
		try {
			Iterable<Tuple2> values = list.get();
			if (values == null || !values.iterator().hasNext()) {
				// return null when the list is empty
				return null;
			} else {
				//values.
				ArrayList<Tuple2> tuples = Lists.newArrayList(values);
				tuples.sort((o1, o2) -> {
					int n = Math.min(o1.orderKeys.size(), o2.orderKeys.size());
					for (int i = 0; i < n; i++) {
						Comparable e1 = o1.orderKeys.get(i);
						Comparable e2 = o2.orderKeys.get(i);
						if (e1 == null && e2 == null) {
							continue;
						}
						if (e1 == null) {
							return -1;
						}
						if (e2 == null) {
							return 1;
						}
						int t = e1.compareTo(e2);
						if (t == 0) {
							continue;
						}
						return t;
					}
					int t = Integer.compare(o1.orderKeys.size(), o2.orderKeys.size());
//					if (t != 0) {
//						return t;
//					}
//					if (o1.data == null) {
//						return -1;
//					}
//					if (o2.data == null) {
//						return 1;
//					}
//					if (Comparable.class.isAssignableFrom(o1.data.getClass()) && Comparable.class.isAssignableFrom(o2.data.getClass())) {
//						return ((Comparable) o1.data).compareTo((Comparable) o2.data);
//					}
					return t;
				});
				String outStr = JSON.toJSONString(
						tuples
								.stream()
								.map(e -> e.data)
								.collect(Collectors.toList())
				);
				return outStr;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return "[]";
	}

	public void resetAccumulator(GenericRow acc) {
		ListView<Tuple2> list = (ListView<Tuple2>) acc.getField(0);
		ListView<Tuple2> retractList = (ListView<Tuple2>) acc.getField(1);
		list.clear();
		retractList.clear();
	}

	@Override
	public DataType getResultType() {
		return DataTypes.STRING;
	}

	/*public static void main(String[] args) throws Exception {
		CollectToJsonAggFunction s = new CollectToJsonAggFunction();

		GenericRow acc = new GenericRow(2);
		// adding list
		acc.update(0, new ListView<Tuple2>());

		s.accumulate(acc, "1", 2);
		s.accumulate(acc, "2", 1);
		s.accumulate(acc, "4");
		s.accumulate(acc, "3");
		s.accumulate(acc, "5", -1);
		s.accumulate(acc, "6", null, null);
		s.accumulate(acc, "7", null);
		s.accumulate(acc, (String)null, 0);

		byte[] result = s.getValue(acc);
		System.out.println(new String(result, "UTF-8"));

//		String ss = "[\"1\",\"1\"]";

//        byte[] result1 = new byte[]{91, 49, 52, 54, 52, 49, 49, 54, 44, 49, 52, 54, 52, 49, 49, 54, 44, 49, 52, 54, 52, 49, 49, 54, 93};
//        byte[] result2 = new byte[]{91, 52, 51, 51, 57, 55, 50, 50, 93};
//        System.out.println(new String(result1,"UTF-8") );
//        System.out.println(new String(result2,"UTF-8") );
//
//        JSONArray array =JSON.parseArray(new String(result1,"UTF-8"));
//        array.toArray();

	}*/

	public static void main(String[] args){

		Map<Character, String> escapes;
		escapes = new HashMap<>();
		escapes.put('\\', "\\\\");
		escapes.put('\n', "\\n");
		escapes.put('\t', "\\t");
		escapes.put('\b', "\\b");
		escapes.put('\f', "\\f");
		escapes.put('\r', "\\r");
		escapes.put('\0', "\\0");
		escapes.put('\'', "\\'");
		escapes.put('`', "\\`");

		List<Integer> list = new ArrayList<Integer>();
		//list.add(2);
		//list.add(3);
		//list.add(1);

		String outStr = JSON.toJSONString(list);

		System.out.println(outStr);

		StringBuilder sb = new StringBuilder(outStr.length() + 5);
		boolean changed = false;
		for (int i = 0; i < outStr.length(); i++) {
			char c = outStr.charAt(i);
			if (escapes.containsKey(c)) {
				sb.append(escapes.get(c));
			} else if (c == '"') { // " may be changed to '
				boolean needChange = outStr.charAt(i - 1) != '\\';
				sb.append(needChange ? '\'' : c);
				changed |= needChange;
			} else {
				sb.append(c);
			}
		}
		System.out.println(sb.toString());
	}
}
