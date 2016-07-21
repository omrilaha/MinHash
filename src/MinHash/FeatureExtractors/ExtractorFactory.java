package MinHash.FeatureExtractors;

import java.io.IOException;

public interface ExtractorFactory {
    Extractor create(String filename) throws IOException;
}
