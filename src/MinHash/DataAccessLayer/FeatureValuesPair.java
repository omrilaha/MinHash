package MinHash.DataAccessLayer;

import MinHash.FeatureExtractors.Feature;

public class FeatureValuesPair {
    public Feature feature;
    public long[] hashValues;

    public FeatureValuesPair(Feature feature, long[] hashValues) {
        this.feature = feature;
        this.hashValues = hashValues;
    }
}
