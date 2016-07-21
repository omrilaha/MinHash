import MinHash.DataAccessLayer.H2.H2Signatures;
import MinHash.DataAccessLayer.H2.H2Vocabulary;
import MinHash.DataAccessLayer.MapDB.MapDBSignatures;
import MinHash.DataAccessLayer.MapDB.MapDBVocabulary;
import MinHash.FeatureExtractors.ByteExtractorFactory;
import MinHash.Scheme;
import MinHash.Signatures.ByteHashFunctionFactory;

import java.util.Vector;

public class MinHashScheme {

    public static void main(String[] args) throws Exception {

        H2Signatures h2Signatures = new H2Signatures(100, false);
        // h2Signatures.CreateTable();

        H2Vocabulary h2Vocabulary = new H2Vocabulary(100, false);
        // h2Vocabulary.CreateTable();

        Scheme scheme = new Scheme(
                new ByteHashFunctionFactory(),
                100,
                new ByteExtractorFactory(64, 8),
                h2Signatures,
                h2Vocabulary
        );

//        Scheme scheme = new Scheme(
//                new ByteHashFunctionFactory(),
//                100,
//                new ByteExtractorFactory(64, 8),
//                new MapDBSignatures("signatures"),
//                new MapDBVocabulary("mapDBVocabulary")
//        );

        Vector<String> files = new Vector<String>();
        for (int i = 89; i <= 96; i++) {
            files.add("C:/Users/omrilaha/IdeaProjects/MinHashing/src/dumps/MalSnap-IIS-Snapshot" + i + ".dmp");
//            files.add("C:/Users/omrilaha/IdeaProjects/MinHashing/src/" + i + ".vmem");
        }
        scheme.run(files);
    }
}
