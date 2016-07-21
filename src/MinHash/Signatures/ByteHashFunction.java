package MinHash.Signatures;

import MinHash.FeatureExtractors.Feature;
import MinHash.Utils.GeneralUtils;

import java.util.concurrent.ThreadLocalRandom;

public class ByteHashFunction implements HashFunction {
    private int prime = 982451653;
    private int funcNum;
    private long a;
    private long b;

    public ByteHashFunction(int funcNum) {
        this.a = new Long(String.valueOf(ThreadLocalRandom.current().nextInt(1, this.prime)));
        this.b = new Long(String.valueOf(ThreadLocalRandom.current().nextInt(0, this.prime-1)));
        this.funcNum = funcNum;
    }

    public long hash(Feature feature) {
        long featureInt = new Long(GeneralUtils.bytesToLong(feature.getBytes()));
        long x = this.a * featureInt;
        x += this.b;
        x = x % new Long(String.valueOf(this.prime));

        return x;
        //return ((feature.hashCode() % this.prime) + (this.funcNum * (feature.hashCode())) % this.prime);
    }
}