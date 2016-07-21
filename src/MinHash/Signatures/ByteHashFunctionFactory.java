package MinHash.Signatures;

public class ByteHashFunctionFactory implements HashFunctionsFactory {
    private int funcCounter;

    public ByteHashFunctionFactory() {
        this.funcCounter = 0;
    }

    public HashFunction create() {
        return new ByteHashFunction(this.funcCounter++);
    }
}
