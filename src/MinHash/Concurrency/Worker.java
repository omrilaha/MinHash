package MinHash.Concurrency;

import MinHash.DataAccessLayer.FeatureValuesPair;
import MinHash.DataAccessLayer.H2.H2Vocabulary;
import MinHash.FeatureExtractors.Feature;
import MinHash.Signatures.HashFunction;
import MinHash.Signatures.Signature;

import java.util.ArrayList;

public class Worker implements Runnable {
    private ArrayList<HashFunction> hashFunctions;
    private H2Vocabulary h2Vocabulary;
    private FileWorker fileWorker;
    private VocabularyWorker vocabularyWorker;
    private Signature signature;
    private int exists = 0;
    private SynchronizedSigner signer;

    public Worker(Signature signature, FileWorker fileWorker, VocabularyWorker vocabularyWorker, SynchronizedSigner signer) {
        this.signature = signature;
        this.hashFunctions = signature.hashFunctions;
        this.h2Vocabulary = signature.h2Vocabulary;
        this.fileWorker = fileWorker;
        this.vocabularyWorker = vocabularyWorker;
        this.signer = signer;
    }

    public void run() {
        try {
            while (!this.fileWorker.isEndOfFile()) {
                work();
            }
            if (this.fileWorker.isEndOfFile()) {
                while (this.fileWorker.getQueueSize() > 0) {
                    work();
                }
            }
            if (this.fileWorker.isEndOfFile() && this.fileWorker.getQueueSize() == 0) {
                System.out.println("worker finish");
                this.signature.sign();
            }
        } catch (Exception e) {
            e.printStackTrace();
            // this.h2Vocabulary.close();
        }
    }

    private void work() throws Exception {
        Feature feature = this.fileWorker.get();
        if (!this.h2Vocabulary.IsExists(feature.toString())) {
            long[] hashValues = new long[this.hashFunctions.size()];
            for (int i = 0; i < this.hashFunctions.size(); i++) {
                hashValues[i] = this.hashFunctions.get(i).hash(feature);
            }

            this.vocabularyWorker.put(new FeatureValuesPair(feature, hashValues));
            this.signer.sign(hashValues);
        } else {
            this.signer.sign(this.h2Vocabulary.Get(feature.toString()));
        }
    }
}
