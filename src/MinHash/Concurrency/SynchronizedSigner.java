package MinHash.Concurrency;

public class SynchronizedSigner {
    private long[] signiture;

    public SynchronizedSigner(long[] signiture) {
        this.signiture = signiture;
    }

    public synchronized void sign(long[] values) {
        for (int i = 0; i < signiture.length; i++) {
            if (values[i] < signiture[i]) {
                signiture[i] = values[i];
            }
        }
    }
}
