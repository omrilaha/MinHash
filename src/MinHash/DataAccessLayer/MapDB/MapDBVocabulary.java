package MinHash.DataAccessLayer.MapDB;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import java.util.concurrent.ConcurrentMap;

public class MapDBVocabulary implements MapDB <long[]> {
    private DB db;
    private ConcurrentMap<String, long[]> map;

    public MapDBVocabulary(String DBName) throws Exception {
        this.db = DBMaker.fileDB(DBName + ".db").checksumHeaderBypass().transactionEnable().make();
        createOrOpenMap("mapDBVocabulary");
    }

    public void createOrOpenMap(String mapName) {
        this.map = db.hashMap(mapName).keySerializer(Serializer.STRING).valueSerializer(Serializer.LONG_ARRAY).createOrOpen();
    }

    public void close() {
        this.db.close();
    }

    public long[] get(String key) {
        return this.map.get(key);
    }

    public boolean isKeyExists(String key) {
        return this.map.containsKey(key);
    }

    public void add(String key, long[] vector) {
       this.map.put(key, vector);
    }

    public void commit() {
        this.db.commit();
    }
}

