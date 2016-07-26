package MinHash;

import MinHash.DataAccessLayer.H2.H2Signatures;
import MinHash.DataAccessLayer.H2.H2Vocabulary;
import MinHash.FeatureExtractors.ExtractorFactory;
import MinHash.Signatures.HashFunctionsFactory;
import MinHash.Signatures.HashFunctionsGenerator;
import MinHash.Signatures.Signature;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Scheme {

    private int signatureSize;
    private HashFunctionsFactory factory;
    private ExtractorFactory extractorFactory;
    private H2Signatures h2Signatures;
    private H2Vocabulary h2Vocabulary;

    private Vector<Signature> signatures;
    private Vector<String> errorFileNames;

    public Scheme(HashFunctionsFactory factory, int signatureSize, ExtractorFactory extractorFactory, H2Signatures h2Signatures, H2Vocabulary h2Vocabulary) {
        this.factory = factory;
        this.signatureSize = signatureSize;
        this.extractorFactory = extractorFactory;
        this.h2Signatures = h2Signatures;
        this.h2Vocabulary = h2Vocabulary;

        this.signatures = new Vector<Signature>();
        this.errorFileNames = new Vector<String>();
    }

    public void run(Vector<String> fileNames) throws SQLException {
        int poolSize = 4;
        ExecutorService executor = Executors.newFixedThreadPool(poolSize);
        ExecutorService managers = Executors.newFixedThreadPool(fileNames.capacity());
        for (String filename : fileNames) {
            try {
                addDocument(filename, executor, managers);
            } catch (IOException e) {
                this.errorFileNames.add(filename);
            }
        }

        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        }

        managers.shutdown();
        try {
            managers.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        }

        System.out.println("------- FINISH ------");
    }

    private void addDocument(String filename, ExecutorService executor, ExecutorService managers) throws IOException{
        this.signatures.add(new Signature(
                filename,
                new HashFunctionsGenerator(this.factory).generateArray(this.signatureSize),
                this.extractorFactory.create(filename),
                this.h2Signatures,
                this.h2Vocabulary,
                executor,
                managers
        ));
    }
}
