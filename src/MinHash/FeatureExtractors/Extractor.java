package MinHash.FeatureExtractors;

import MinHash.Utils.EndOfFileException;

import java.io.IOException;

public interface Extractor {
    void init() throws IOException;
    // Vector<Feature> extractAll();
    Feature ExtractNext() throws EndOfFileException;
}
