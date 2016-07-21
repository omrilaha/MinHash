package MinHash.FeatureExtractors;

import java.io.IOException;

public class ByteExtractorFactory implements ExtractorFactory {
    private int windowSize;
    private int stepSize;

    public ByteExtractorFactory(int windowSize, int stepSize) {
        this.windowSize = windowSize;
        this.stepSize = stepSize;
    }

    public ByteExtractor create(String filename) throws IOException {
        return new ByteExtractor(filename, this.windowSize, this.stepSize);
    }
}
