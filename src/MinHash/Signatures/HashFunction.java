package MinHash.Signatures;

import MinHash.FeatureExtractors.Feature;

public interface HashFunction {
    long hash(Feature feature);
}
