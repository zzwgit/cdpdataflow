package io.infinivision.flink.udfs;

import org.apache.flink.table.api.functions.ScalarFunction;

import java.util.Arrays;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

// 对 uuid string 空间 分区
public class UUIDPartitionFunction extends ScalarFunction {
	private int level = -1;
	private double step;
	private int[] randomInts;  // 每个partition 的 随机编号, 默认400w个(16M的大小), 所以partition数目不要超过400w
	// 防止为null和blank的全部落到同一个slot
	private int nullBlankCount;

	public UUIDPartitionFunction() {
		int scale = 10;  // 放大倍数
		int total = 4 * 1000 * 1000 * scale; // 候选集 多一点, 然后丛中选取部分,这样数字更加分散
		int[] tmp = new int[total];  // 候选数字集
		for (int j = 0; j < total; j++) {
			tmp[j] = j;
		}
		// shuffle 多次
		shuffleArray(tmp);
		shuffleArray(tmp);
		shuffleArray(tmp);
		shuffleArray(tmp);
		randomInts = new int[total / scale];
		// 选取前100w个数字
		System.arraycopy(tmp, 0, randomInts, 0, total / scale);
	}

	// partition必须小于 4百万
	public int eval(String mid, int partition) {
		return eval(mid, partition, 0);
	}

	public int eval(String mid, int partition, int isPartitionNumEqualToSlotNum) {
		if (mid == null || mid.length() == 0) return nullBlankCount++;

		// 计算 partition 对应到 mid前几位字符
		if (level == -1) {
			synchronized (this) {
				if (level == -1) {
					calcLevel(partition);
				}
			}
		}
		// 截取 mid 前 level个字符计算在哪个partition
		int l = Math.min(level, mid.length());
		char[] chars = new char[l];
		int j = 0;
		for (int i = 0; i < l; i++) {
			char c = mid.charAt(i);
			if (isHex(c)) {
				chars[j++] = c;
			}
		}
		while (j < chars.length) {
			chars[j++] = '0';
		}
		double value = Integer.parseInt(new String(chars), 16) / step;
		int index = (int) Math.floor(value);
		if (index == partition) {
			index--;
		}
		if (isPartitionNumEqualToSlotNum == 1) {
			return index;
		}
		return randomInts[index];
	}

	private boolean isHex(char c) {
		return '0' <= c && c <= '9' ||
				'a' <= c && c <= 'f' ||
				'A' <= c && c <= 'F';
	}

	// 0 - 16 => 0
	// 30 => 1
	private void calcLevel(int partition) {
		int i = 1;
		int t = partition;
		while ((t >>= 4) > 0) {
			i += 1;
		}
		level = i;
		char[] chars = new char[level];
		Arrays.fill(chars, 'f');
		int max = Integer.parseInt(new String(chars), 16) + 1;
		step = max * 1.0 / partition;
	}

	private static void shuffleArray(int[] ar) {
		Random rnd = ThreadLocalRandom.current();
		for (int i = ar.length - 1; i > 0; i--) {
			int index = rnd.nextInt(i + 1);
			// Simple swap
			int a = ar[index];
			ar[index] = ar[i];
			ar[i] = a;
		}
	}

	public static void main(String[] args) {
		UUIDPartitionFunction f = new UUIDPartitionFunction();
		System.out.println("\\N");
		for (int i = 0; i < 20; i++) {
			String uuid = UUID.randomUUID().toString();
			int a = f.eval(uuid, 45, 1);
			System.out.print(uuid);
			System.out.print("  ");
			System.out.println(a);
		}
	}

}
