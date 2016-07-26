package MinHash.DataAccessLayer.H2;

import java.sql.*;

public class H2Vocabulary extends H2DB {
    private final String columnNames = "HASH";
    private final String tableName = "VOCABULARY";

    private int signatureSize;
    private String InsertPreparedQuery;

    public H2Vocabulary(int signatureSize) throws SQLException{
        this.signatureSize = signatureSize;
        init();
    }

    public void CreateVocabularyTable() throws SQLException {
        String columnNames = "FEATURE varchar(400) primary key, ";
        for (int i = 0; i < this.signatureSize - 1; i++) {
            columnNames += this.columnNames + i + " BIGINT, ";
        }
        columnNames += this.columnNames + (this.signatureSize - 1) + " BIGINT";

        CreateTable(this.tableName, columnNames);
    }

    public long[] Get(String feature) throws SQLException {
        Connection connection = getDBConnection();
        PreparedStatement preparedStatement = null;
        long[] featureSignature = null;

        String query = "SELECT * FROM " + this.tableName + " WHERE FEATURE=?";
        try {
            connection.setAutoCommit(false);

            preparedStatement = connection.prepareStatement(query);
            preparedStatement.setString(1, feature);
            ResultSet rs = preparedStatement.executeQuery();

            featureSignature = new long[this.signatureSize];
            while (rs.next()) {
                for (int i = 0; i < this.signatureSize; i++) {
                    featureSignature[i] = rs.getLong(this.columnNames + String.valueOf(i));
                }
            }
            preparedStatement.close();

            connection.commit();
        } catch (SQLException e) {
            System.out.println("Vocabulary Get - Exception Message " + e.getLocalizedMessage());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            connection.close();
        }

        return featureSignature;
    }

    public boolean IsExists(String feature) throws SQLException {
        Connection connection = getDBConnection();
        PreparedStatement preparedStatement = null;
        boolean exists = false;

        String query = "SELECT COUNT(*) FROM " + this.tableName + " WHERE FEATURE=?";
        try {
            connection.setAutoCommit(false);

            preparedStatement = connection.prepareStatement(query);
            preparedStatement.setString(1, feature);
            ResultSet rs = preparedStatement.executeQuery();

            if (rs.next()) {
                long result = rs.getLong("COUNT(*)");
                exists = result > 0;
            }
            preparedStatement.close();

            connection.commit();
        } catch (SQLException e) {
            System.out.println("Vocabulary IsExists - Exception Message " + e.getLocalizedMessage());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            connection.close();
        }

        return exists;
    }

    private void init() {
        generateInsertPreparedQuery();
    }

    private void generateInsertPreparedQuery() {
        String columns = "FEATURE, ";
        String preparedValues = "";
        for (int i = 0; i < this.signatureSize - 1; i++) {
            columns += this.columnNames + i + ", ";
            preparedValues += "?, ";
        }
        columns += this.columnNames + (signatureSize - 1);
        columns = "(" + columns + ")";
        preparedValues += "?, ?";
        preparedValues = "(" + preparedValues + ")";

        this.InsertPreparedQuery = "INSERT INTO " + tableName  + columns + " VALUES " + preparedValues;
    }

    public void Insert(String feature, long[] signature) throws SQLException {
        Connection connection = getDBConnection();
        PreparedStatement preparedStatement = null;

        try {
            connection.setAutoCommit(false);

            preparedStatement = connection.prepareStatement(this.InsertPreparedQuery);
            preparedStatement.setString(1, feature);
            for (int i = 0; i < signature.length; i++) {
                preparedStatement.setLong(i + 2, signature[i]);
            }

            preparedStatement.executeUpdate();
            preparedStatement.close();

            connection.commit();
        } catch (SQLException e) {
            System.out.println("Vocabulary Insert - Exception Message " + e.getLocalizedMessage());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            connection.close();
        }
    }

}