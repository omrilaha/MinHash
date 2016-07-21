package MinHash.DataAccessLayer.MapDB;

import MinHash.DataAccessLayer.MinHashDB;

public interface MapDB <T> extends MinHashDB <T> {

    void createOrOpenMap(String mapName);

}

