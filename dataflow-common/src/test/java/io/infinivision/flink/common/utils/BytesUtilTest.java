package io.infinivision.flink.common.utils;

import org.junit.Test;
import org.junit.Assert;

import java.util.Arrays;

public class BytesUtilTest {

    @Test
    public void testIntsBytes() {
        Integer[] expecteds = new Integer[]{1234, 5678};
        byte[] intsBytes = BytesUtil.intsToBytes(Arrays.asList(expecteds));
        Integer[] actuals = BytesUtil.bytesToInts(intsBytes);
        Assert.assertArrayEquals(expecteds, actuals);
    }

    @Test
    public void testLongsBytes() {
        Long[] expecteds = new Long[]{1234L, 5678L};
        byte[] longsToBytes = BytesUtil.longsToBytes(Arrays.asList(expecteds));
        Long[] actuals = BytesUtil.bytesToLongs(longsToBytes);
        Assert.assertArrayEquals(expecteds, actuals);
    }

}
