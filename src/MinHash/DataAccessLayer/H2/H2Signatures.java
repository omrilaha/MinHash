package MinHash.DataAccessLayer.H2;

import org.h2.command.ddl.CreateTable;

import java.sql.*;

public class H2Signatures extends H2DB {
    private final String columnNames = "hash";
    private final String tableName = "SIGNATURES";
    private int size;

    public H2Signatures(int size) throws SQLException{
        this.size = size;
    }

    public void CreateSignaturesTable() throws SQLException {
        String columnNames = "filename varchar(400) primary key, ";
        for (int i = 0; i < this.size - 1; i++) {
            columnNames += this.columnNames + i + " BIGINT, ";
        }
        columnNames += this.columnNames + (this.size - 1) + " BIGINT";

        CreateTable(this.tableName, columnNames);
    }

//
//
//    public long[] get(String filename) throws SQLException {
//        String statement = "SELECT * FROM " + this.tableName + " WHERE filename=" + filename;
//        long[] signature = new long[this.size];
//
//        ResultSet rs = query(statement);
//
//        rs.next();
//        for (int i = 0; i < this.size; i++) {
//            signature[i] = rs.getLong(this.columnNames + String.valueOf(i));
//        }
//
//        return signature;
//    }

//    public boolean isKeyExists(String key) {
//        return false;
//    }

    public void Insert(String filename, long[] signature) {

        String columnName = "";
        String columnValue = "";

        for (int i = 0; i < signature.length - 1; i++) {
            columnName += this.columnNames + i + ", ";
            columnValue += signature[i] + ", ";
        }
        columnName += this.columnNames + signature.length;
        columnValue += signature[signature.length];
        String statement = "INSERT INTO " + tableName + "(filename, " + columnName + ") VALUES(" + filename + ", " + columnValue;

        execute(statement);


        Connection connection = getDBConnection();
        PreparedStatement preparedStatement = null;

        String columns = "filename, ";
        String preparedValues = "";
        for (int i = 0; i < signature.length - 1; i++) {
            columns += this.columnNames + i + ", ";
            preparedValues += "?, ";
        }
        columns += this.columnNames + signature.length;
        columns = "(" + columns + ")";
        preparedValues += "?";

        String query = "INSERT INTO " + tableName  + columns +  values" + "(?,?)";

        try {
            connection.setAutoCommit(false);

            createPreparedStatement = connection.prepareStatement(CreateQuery);
            createPreparedStatement.executeUpdate();
            createPreparedStatement.close();

            insertPreparedStatement = connection.prepareStatement(InsertQuery);
            insertPreparedStatement.setInt(1, 1);
            insertPreparedStatement.setString(2, "Jose");
            insertPreparedStatement.executeUpdate();
            insertPreparedStatement.close();

            selectPreparedStatement = connection.prepareStatement(SelectQuery);
            ResultSet rs = selectPreparedStatement.executeQuery();
            System.out.println("H2 Database inserted through PreparedStatement");
            while (rs.next()) {
                System.out.println("Id "+rs.getInt("id")+" Name "+rs.getString("name"));
            }
            selectPreparedStatement.close();

            connection.commit();
        } catch (SQLException e) {
            System.out.println("Exception Message " + e.getLocalizedMessage());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            connection.close();
        }
    }

}