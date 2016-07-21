package MinHash.FeatureExtractors;

import MinHash.Utils.GeneralUtils;

import java.util.Arrays;

public class ByteFeature extends Feature {

    private byte[] bytes;
    private long intValue;

    public ByteFeature(byte[] bytes) {
        this.bytes = bytes;
        this.intValue = new Long(GeneralUtils.bytesToLong(bytes));
    }

    public byte[] getBytes() {
        return this.bytes;
    }

    @Override
    public Integer hash() {
        return null;
    }

    @Override
    public String toString() {
        //String str = new String(this.bytes);
        String str = Arrays.toString(this.bytes);
        return str;
    }

    @Override
    public int shortRepr() {
        return this.toString().hashCode();
    }
}
