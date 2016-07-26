package MinHash.DataAccessLayer.H2;

import java.sql.*;

public class H2Signatures extends H2DB {
    private final String columnNames = "HASH";
    private final String tableName = "SIGNATURES";
    private int signatureSize;

    private String InsertPreparedQuery;

    public H2Signatures(int signatureSize) throws SQLException{
        this.signatureSize = signatureSize;
        init();
    }

    public void CreateSignaturesTable() throws SQLException {
        String columnNames = "FILENAME varchar(255) primary key, ";
        for (int i = 0; i < this.signatureSize - 1; i++) {
            columnNames += this.columnNames + i + " BIGINT, ";
        }
        columnNames += this.columnNames + (this.signatureSize - 1) + " BIGINT";

        CreateTable(this.tableName, columnNames);
    }

    private void init() {
        generateInsertPreparedQuery();
    }

    private void generateInsertPreparedQuery() {
        String columns = "FILENAME, ";
        String preparedValues = "";
        for (int i = 0; i < this.signatureSize - 1; i++) {
            columns += this.columnNames + i + ", ";
            preparedValues += "?, ";
        }
        columns += this.columnNames + (signatureSize - 1);
        columns = "(" + columns + ")";
        preparedValues += "?";
        preparedValues = "(" + preparedValues + ")";

        this.InsertPreparedQuery = "INSERT INTO " + tableName  + columns + " VALUES " + preparedValues;
    }

    public void Insert(String filename, long[] signature) throws SQLException {
        Connection connection = getDBConnection();
        PreparedStatement preparedStatement = null;

        try {
            connection.setAutoCommit(false);

            preparedStatement = connection.prepareStatement(this.InsertPreparedQuery);
            preparedStatement.setString(0, filename);
            for (int i = 0; i < signature.length; i++) {
                preparedStatement.setLong(i + 1, signature[i]);
            }

            preparedStatement.executeUpdate();
            preparedStatement.close();

            connection.commit();
        } catch (SQLException e) {
            System.out.println("Signature Insert - Exception Message " + e.getLocalizedMessage());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            connection.close();
        }
    }

}