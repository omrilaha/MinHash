package MinHash.DataAccessLayer;

public interface MinHashDB <T> {
    void close();
    Object get(String key);
    boolean isKeyExists(String key);
    void add(String key, T t);
}
