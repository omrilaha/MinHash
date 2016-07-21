package MinHash;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Vector;

public class DBHandler {
    private String sDriverName = "org.sqlite.JDBC";
    private String sTempDb = "C:/sqlite_databases/test.db";
    private String sJdbc = "jdbc:sqlite";
    private String sDbUrl = sJdbc + ":" + sTempDb;
    private int iTimeout = 30;
    private String tableName;

    private Connection conn;
    //private Statement stmt;

    public DBHandler(String tableName) throws Exception {
        this.conn = DriverManager.getConnection(sDbUrl);
        this.tableName = tableName;
    }

    public void registerDriver() throws Exception{
        // register the driver
        Class.forName(this.sDriverName);
    }
    public void createTable() throws Exception{
        String sMakeTable = "CREATE TABLE " + this.tableName + " (id text PRIMARY KEY, ";
        for (int i = 1; i < 50; i++) {
            sMakeTable += "value" + i + " numeric, ";
        }
        sMakeTable += "value50 text)";

        Statement stmt = this.conn.createStatement();
        stmt.setQueryTimeout(this.iTimeout);
        stmt.executeUpdate( sMakeTable );
        stmt.close();
    }

    public void close() throws Exception{
        this.conn.close();
    }

    public ResultSet readQuery(String query) throws Exception {
        Statement stmt = this.conn.createStatement();
        stmt.setQueryTimeout(this.iTimeout);
        ResultSet rs = stmt.executeQuery(query);
        // stmt.close();

        return rs;
    }

    private void writeQuery(String query) throws Exception {
        Statement stmt = this.conn.createStatement();
        stmt.setQueryTimeout(this.iTimeout);
        stmt.executeUpdate(query);
        stmt.close();
    }

    public void insertVector(String id, Vector<Long> vec) {
        String query = "INSERT INTO " + this.tableName + " VALUES('" + id + "', ";
            for (int i = 0; i < vec.size(); i++) {
                if (i == vec.size() - 1) {
                    query += vec.get(i).toString() + ")";
                } else {
                    query += vec.get(i).toString() + ", ";
                }
            }

        try {
            writeQuery(query);
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public Vector<Long> getVector(String id) {
        try {
            Vector<Long> vec = new Vector<Long>();
            ResultSet result = readQuery("SELECT * FROM " + this.tableName + " WHERE id='" + id + "'");
            while(result.next()) {
                for (int i = 1; i <= 50; i++) {
                    vec.add(result.getLong("value" + i));
                }
            }

            if (vec.size() > 0) {
                return vec;
            } else {
                return null;
            }
        } catch (Exception ignore) {
            return null;
        }
    }

    public boolean isExists(String id) {
        try {
            Vector<Long> vec = new Vector<Long>();
            ResultSet result = readQuery("SELECT COUNT(1) FROM " + this.tableName + " WHERE id='" + id + "'");
            result.next();
            return result.getInt(1) == 1;
        } catch (Exception ignore) {
            return false;
        }
    }

//    public void s() throws Exception {
//        // register the driver
//        String sDriverName = "org.sqlite.JDBC";
//        Class.forName(sDriverName);
//
//        // now we set up a set of fairly basic string variables to use in the body of the code proper
//        String sTempDb = "C:/sqlite_databases/test.db";
//        String sJdbc = "jdbc:sqlite";
//        String sDbUrl = sJdbc + ":" + sTempDb;
//        // which will produce a legitimate Url for SqlLite JDBC :
//        // jdbc:sqlite:hello.db
//        int iTimeout = 30;
//        String sMakeTable = "CREATE TABLE test1 (id text PRIMARY KEY, ";
//        for (int i = 1; i < 50; i++) {
//            sMakeTable += "value" + i + " text, ";
//        }
//        sMakeTable += "value50 text)";
////        String sMakeInsert = "INSERT INTO dummy VALUES(1,'Hello from the database')";
////        String sMakeSelect = "SELECT response from dummy";
//
//        // create a database connection
//        Connection conn = DriverManager.getConnection(sDbUrl);
//        try {
//            Statement stmt = conn.createStatement();
//            try {
//                stmt.setQueryTimeout(iTimeout);
//                stmt.executeUpdate( sMakeTable );
//                // stmt.executeUpdate( sMakeInsert );
////                ResultSet rs = stmt.executeQuery(sMakeSelect);
////                try {
////                    while(rs.next())
////                    {
////                        String sResult = rs.getString("response");
////                        System.out.println(sResult);
////                    }
////                } finally {
////                    try { rs.close(); } catch (Exception ignore) {}
////                }
//            } finally {
//                try { stmt.close(); } catch (Exception ignore) {}
//            }
//        } finally {
//            try { conn.close(); } catch (Exception ignore) {}
//        }
//    }
}
