package MinHash.Concurrency;

import MinHash.FeatureExtractors.Extractor;
import MinHash.FeatureExtractors.Feature;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class FileWorker implements Runnable {
    private Extractor extractor;
    private BlockingQueue<Feature> queue;
    private boolean endOfFile;

    public FileWorker(Extractor extractor) {
        this.extractor = extractor;
        this.queue = new ArrayBlockingQueue<Feature>(100000);
        this.endOfFile = false;
    }

    public boolean isEndOfFile() {
        return this.endOfFile;
    }

    public int getQueueSize() {
        int size = this.queue.size();
        //if (size % 1000 == 0) {
            //System.out.println(size);
        //}
        return size;
    }

    public Feature get() {
        try {
            return this.queue.take();
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
            return null;
        }
    }

    public void run() {
        try {
            while (!endOfFile) {
                Feature feature = extractor.ExtractNext();
                if (feature != null) {
                    this.queue.put(feature);
                } else {
                    endOfFile = true;
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

}
