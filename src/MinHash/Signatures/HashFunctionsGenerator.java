package MinHash.Signatures;

import java.util.ArrayList;

public class HashFunctionsGenerator {

    private HashFunctionsFactory factory;

    public HashFunctionsGenerator(HashFunctionsFactory factory) {
        this.factory = factory;
    }
    
    public ArrayList<HashFunction> generateArray(int amount) {
        ArrayList<HashFunction> hashFunctions = new ArrayList<HashFunction>();
                
        for (int i = 0; i < amount; i++) {
            hashFunctions.add(generateFunction(i));
        }

        return hashFunctions;
    }

    private HashFunction generateFunction(int i) {
        return factory.create();
    }
}
