package MinHash.FeatureExtractors;

import MinHash.FileHandler;
import MinHash.Utils.EndOfFileException;

import java.io.IOException;

public class ByteExtractor implements Extractor {
    private FileHandler fileHandler;
    private String filename;
    private int windowSize;
    private int stepSize;

    public ByteExtractor(String filename, int windowSize, int stepSize) throws IOException {
        this.filename = filename;
        this.windowSize = windowSize;
        this.stepSize = stepSize;

        this.fileHandler = new FileHandler(this.filename, this.windowSize, this.stepSize);
    }

    public void init() throws IOException {
        this.fileHandler = new FileHandler(this.filename, this.windowSize, this.stepSize);
    }

//    public Vector<Feature> extractAll() {
//        ByteBuffer[] chunk;
//        Vector<Feature> features = new Vector<>();
//
//        while ((chunk = this.fileHandler.readNextChunk()) != null) {
//            features.add(new ByteFeature(chunk));
//        }
//
//        this.fileHandler.close();
//        return features;
//    }

    public Feature ExtractNext() throws EndOfFileException {
        byte[] chunk = fileHandler.ReadNextWindow();
        if (chunk != null) {
            return new ByteFeature(chunk);
        } else {
            fileHandler.close();
            return null;
        }
    }
}

