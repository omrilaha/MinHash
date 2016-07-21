package MinHash.Signatures;

import MinHash.Concurrency.FileWorker;
import MinHash.Concurrency.SynchronizedSigner;
import MinHash.Concurrency.VocabularyWorker;
import MinHash.Concurrency.Worker;
import MinHash.DataAccessLayer.H2.H2Signatures;
import MinHash.DataAccessLayer.H2.H2Vocabulary;
import MinHash.FeatureExtractors.Extractor;
import MinHash.FeatureExtractors.Feature;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;

public class Signature {
    private String filename;
    private long[] signature;
    public ArrayList<HashFunction> hashFunctions;
    private H2Signatures h2Signatures;
    public H2Vocabulary h2Vocabulary;
    private boolean signed;
    private int workersCounter;
    private long start;
    private long end;
    private boolean finish;
    private ExecutorService executorService;
    private SynchronizedSigner signer;
    private ExecutorService managers;
    private FileWorker fileWorker;
    private VocabularyWorker vocabularyWorker;

    public Signature(String filename, ArrayList<HashFunction> hashFunctions, Extractor extractor, H2Signatures h2Signatures, H2Vocabulary h2Vocabulary, ExecutorService executor, ExecutorService managers) {
        this.filename = filename;
        this.signature = new long[hashFunctions.size()];
        initSignature();
        this.hashFunctions = hashFunctions;
        this.h2Signatures = h2Signatures;
        this.h2Vocabulary = h2Vocabulary;
        this.signed = false;
        this.start = System.currentTimeMillis();
        this.finish = false;
        this.executorService = executor;
        this.workersCounter = 4;
        this.signer = new SynchronizedSigner(this.signature);
        this.managers = managers;

        Worker[] workers = new Worker[workersCounter];

        this.fileWorker = new FileWorker(extractor);
        this.vocabularyWorker = new VocabularyWorker(this.h2Vocabulary, this.filename);

        for (int i = 0; i < workers.length; i++) {
            workers[i] = new Worker(this, fileWorker, vocabularyWorker, this.signer);
        }

        for (int i = 0; i < workers.length; i++) {
            executor.execute(workers[i]);
        }
        managers.execute(fileWorker);
        managers.execute(vocabularyWorker);
    }

    private void initSignature() {
        for (int i = 0; i < this.signature.length; i++) {
            this.signature[i] = (long)Double.POSITIVE_INFINITY;
        }
    }

    public synchronized void sign() throws SQLException {
        this.workersCounter--;
        if (this.workersCounter == 0 && !this.finish) {
            this.finish = true;
            this.signed = true;
            System.out.println(this.filename + " SIGN!!!");
            this.end = System.currentTimeMillis();
            System.out.println(this.filename + " TOTAL TIME:" + (this.end - this.start));

            this.vocabularyWorker.finish();

            String printedSignature = "";
            for (int i = 0; i < this.signature.length; i++) {
                printedSignature += "[" + this.signature[i] + "],";
            }
            System.out.println("-------------------------------\nSIGNATURE OF: " + this.filename + "\n" + printedSignature + "\n--------------------------");
            this.h2Signatures.Insert(this.filename, this.signature);
        }
    }

    private void addFeature(Feature feature) throws SQLException{
        long[] hashValues = new long[this.hashFunctions.size()];
        for (int i = 0; i < this.hashFunctions.size(); i++) {
            hashValues[i] = this.hashFunctions.get(i).hash(feature);
        }

        this.h2Vocabulary.Insert(feature.toString(), hashValues);

        int index = 0;
        for (Long hashValue : hashValues) {
            if (hashValue < signature[index]) {
                signature[index] = hashValue;
            }

            index++;
        }
    }
}
