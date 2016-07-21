package MinHash.DataAccessLayer.H2;

import java.sql.*;

public class H2Vocabulary extends H2 {
    private final String columnNames = "hash";
    private final String tableName = "VOCABULARY";

    public H2Vocabulary(int size, boolean quick) throws SQLException{
        super(size, quick);
    }

    public void CreateTable() throws SQLException {
        String columnNames = "";
        for (int i = 0; i < this.size - 1; i++) {
            columnNames += this.columnNames + i + " BIGINT, ";
        }
        columnNames += this.columnNames + (this.size - 1) + " BIGINT";
        String statement = "CREATE TABLE " + tableName + "(feature varchar(400) primary key, " + columnNames +")";

        execute(statement);
        commit();
    }

    public long[] get(String feature) throws SQLException {
        String statement = "SELECT * FROM " + this.tableName + " WHERE feature='" + feature + "'";
        long[] featureSignature = new long[this.size];

        ResultSet rs = query(statement);

        rs.next();
        for (int i = 0; i < this.size; i++) {
            featureSignature[i] = rs.getLong(this.columnNames + String.valueOf(i));
        }

        return featureSignature;
    }

    public boolean isKeyExists(String feature) throws SQLException {
        String stringFeature = feature.toString();
        ResultSet rs = query("SELECT COUNT(*) FROM " + this.tableName + " WHERE feature='" + stringFeature + "'");

        if(rs.next()) {
            long result = rs.getLong("COUNT(*)");
            return result > 0;
        }

        return false;
    }

    public void add(String feature, long[] signature) throws SQLException{
        String columnName = "";
        String columnValue = "'";
        //String variables = "";

        for (int i = 0; i < signature.length - 1; i++) {
            columnName += this.columnNames + i + ", ";
            columnValue += signature[i] + "', '";
            //variables += "?,";
        }
        columnName += this.columnNames + (signature.length - 1);
        //variables += "?, ?";
        columnValue += signature[signature.length - 1] + "'";
        String statement = "INSERT INTO " + tableName + "(feature, " + columnName + ") VALUES('" + feature + "', " + columnValue + ")";

//        String preparedStatementString = "INSERT INTO " + tableName + "(feature, " + columnName + ") VALUES(" + variables + ")";
//
//        try {
//            PreparedStatement preparedStatement = this.connection.prepareStatement(preparedStatementString);
//            preparedStatement.setString(1, feature);
//            for (int i = 0; i < signature.length; i++) {
//                preparedStatement.setLong(i + 2, signature[i]);
//            }
//            preparedStatement.executeUpdate();
//            preparedStatement.close();
//            this.commit();
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
        execute(statement);
    }

}