package MinHash.FeatureExtractors;

public abstract class Feature<T> {
    private T feature;

    abstract Integer hash();
    abstract public byte[] getBytes();
    abstract public int shortRepr();
}
