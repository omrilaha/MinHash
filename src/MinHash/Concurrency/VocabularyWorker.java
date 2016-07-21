package MinHash.Concurrency;

import MinHash.DataAccessLayer.FeatureValuesPair;
import MinHash.DataAccessLayer.H2.H2Vocabulary;
import MinHash.DataAccessLayer.MapDB.MapDBVocabulary;

import java.sql.SQLException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class VocabularyWorker implements Runnable {
    private BlockingQueue<FeatureValuesPair> queue;
    private H2Vocabulary h2Vocabulary;
    private int counter;
    private String filename;
    private boolean run;

    public VocabularyWorker(H2Vocabulary h2Vocabulary, String filename) {
        this.filename = filename;
        this.h2Vocabulary = h2Vocabulary;
        this.queue = new ArrayBlockingQueue<FeatureValuesPair>(10000);
        this.counter = 1000;
        this.run = true;
    }

    public void put(FeatureValuesPair featureValuesPair) throws InterruptedException {
        this.queue.put(featureValuesPair);
    }

    public void run() {
        try {
            while(this.run) {
                FeatureValuesPair featureValuesPair = this.queue.take();
                // System.out.println("MapDBVocabulary queue: " +this.queue.size());
                this.h2Vocabulary.add(featureValuesPair.feature.toString(), featureValuesPair.hashValues);
                this.counter--;
                if (this.counter == 0) {
                    System.out.println(this.filename + " COMMIT 10000 FEATURES");
                    this.h2Vocabulary.commit();
                    this.counter = 10000;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void finish() {
        this.run = false;
    }
}
