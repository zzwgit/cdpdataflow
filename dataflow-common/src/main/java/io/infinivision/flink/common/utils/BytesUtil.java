package io.infinivision.flink.common.utils;

import com.google.common.collect.Lists;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.util.BinaryRowUtil;
import org.apache.flink.table.typeutils.BaseRowSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * A helper class for Table Service.
 */
public final class BytesUtil {

    private BytesUtil() {}

    public static int bytesToInt(byte[] bytes) {
        return bytesToInt(bytes, 0);
    }

    public static int bytesToInt(byte[] bytes, int offset) {
        int ans = 0;
        ans |= (bytes[offset] & 0xff) << 24;
        ans |= (bytes[offset + 1] & 0xff) << 16;
        ans |= (bytes[offset + 2] & 0xff) << 8;
        ans |= (bytes[offset + 3] & 0xff);
        return ans;
    }

    public static Integer[] bytesToInts(byte[] bytes) {
        Integer[] ints = new Integer[bytes.length/4];
        for (int offset=0; offset<bytes.length; offset+=4) {
            ints[offset/4] = bytesToInt(bytes, offset);
        }
        return ints;
    }

    public static Long[] bytesToLongs(byte[] bytes) {
        Long[] longs = new Long[bytes.length/8];
        for (int offset=0; offset<bytes.length; offset+=8) {
            longs[offset/8] = bytesToLong(bytes, offset);
        }
        return longs;
    }

    public static Long bytesToLong(byte[] bytes) {
        return bytesToLong(bytes, 0);
    }

    public static Long bytesToLong(byte[] bytes, int offset) {
        long ans = 0;
        ans |= (long) (bytes[offset] & 0xff) << 56;
        ans |= (long)(bytes[offset + 1] & 0xff) << 48;
        ans |= (long)(bytes[offset + 2] & 0xff) << 40;
        ans |= (long)(bytes[offset + 3] & 0xff) << 32;
        ans |= (long)(bytes[offset + 4] & 0xff) << 24;
        ans |= (long)(bytes[offset + 5] & 0xff) << 16;
        ans |= (long)(bytes[offset + 6] & 0xff) << 8;
        ans |= (long)(bytes[offset + 7] & 0xff);
        return ans;
    }

    public static byte[] intToBytes(int x) {
        byte[] buffer = new byte[4];
        buffer[0] = (byte) ((x >> 24) & 0xff);
        buffer[1] = (byte) ((x >> 16) & 0xff);
        buffer[2] = (byte) ((x >> 8) & 0xff);
        buffer[3] = (byte) (x & 0xff);
        return buffer;
    }

    public static byte[] longToBytes(long x) {
        byte[] buffer = new byte[8];
        buffer[0] = (byte) ((x >> 56) & 0xff);
        buffer[1] = (byte) ((x >> 48) & 0xff);
        buffer[2] = (byte) ((x >> 40) & 0xff);
        buffer[3] = (byte) ((x >> 32) & 0xff);
        buffer[4] = (byte) ((x >> 24) & 0xff);
        buffer[5] = (byte) ((x >> 16) & 0xff);
        buffer[6] = (byte) ((x >> 8) & 0xff);
        buffer[7] = (byte) (x & 0xff);
        return buffer;
    }

    public static byte[] intsToBytes(Collection<Integer> ints) {
        if (ints == null || ints.isEmpty()) {
            return new byte[0];
        }
        byte[] buffer = new byte[Integer.BYTES * ints.size()];
        int index = 0;
        for (Integer x : ints) {
            byte[] bytes = intToBytes(x);
            System.arraycopy(bytes, 0, buffer, index * Integer.BYTES, Integer.BYTES);
            index++;
        }
        return buffer;
    }

    public static byte[] longsToBytes(Collection<Long> longs) {
        if (longs == null || longs.isEmpty()) {
            return new byte[0];
        }
        byte[] buffer = new byte[Long.BYTES * longs.size()];
        int index = 0;
        for (Long x : longs) {
            byte[] bytes = longToBytes(x);
            System.arraycopy(bytes, 0, buffer, index * Long.BYTES, Long.BYTES);
            index++;
        }
        return buffer;
    }

    public static byte[] serialize(BaseRow baseRow, BaseRowSerializer baseRowSerializer) {
        BinaryRow binaryRow;
        if (baseRow.getClass() == BinaryRow.class) {
            binaryRow = (BinaryRow) baseRow;
        } else {
            try {
                binaryRow = baseRowSerializer.baseRowToBinary(baseRow);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        byte[] rowBytes = BinaryRowUtil.copy(binaryRow.getAllSegments(), binaryRow.getBaseOffset(), binaryRow.getSizeInBytes());

        byte[] buffer = new byte[binaryRow.getSizeInBytes() + Integer.BYTES];

        byte[] intBytes = intToBytes(binaryRow.getSizeInBytes());

        for (int i = 0; i < Integer.BYTES; i++) {
            buffer[i] = intBytes[i];
        }

        System.arraycopy(rowBytes, 0, buffer, Integer.BYTES, binaryRow.getSizeInBytes());

        return buffer;
    }

    public static BaseRow deSerialize(byte[] buffer, int offset, int sizeInBytes, BaseRowSerializer baseRowSerializer) {
        MemorySegment memorySegment = MemorySegmentFactory.wrap(buffer);

        BinaryRow row = new BinaryRow(baseRowSerializer.getNumFields());
        row.pointTo(memorySegment, offset, sizeInBytes);

        return row;
    }

    public static BaseRow deSerialize(byte[] buffer, int sizeInBytes, BaseRowSerializer baseRowSerializer) {
        return deSerialize(buffer, 0, sizeInBytes, baseRowSerializer);
    }

}
