package MinHash.Concurrency;

public class SynchronizedSigner {
    private long[] signature;

    public SynchronizedSigner(long[] signature) {
        this.signature = signature;
    }

    public synchronized void sign(long[] values) {
        for (int i = 0; i < signature.length; i++) {
            if (values[i] < signature[i]) {
                signature[i] = values[i];
            }
        }
    }
}
