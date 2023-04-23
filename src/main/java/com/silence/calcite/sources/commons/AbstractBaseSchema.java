package com.silence.calcite.sources.commons;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import java.sql.*;
import java.util.List;
import java.util.Properties;

public abstract class AbstractBaseSchema {
    public abstract Schema getSchema(SchemaPlus parentSchema);

    public abstract String getName();

    public abstract List<String> getQuerys();

    private Connection getConnection() throws ClassNotFoundException, SQLException {
        Class.forName("org.apache.calcite.jdbc.Driver");

        Properties properties = new Properties();
        properties.setProperty("lex", "JAVA");

        return DriverManager.getConnection("jdbc:calcite:", properties);
    }


    private Connection addSchema(Connection connection, String name) throws SQLException {
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        rootSchema.add(name, getSchema(rootSchema));
        return calciteConnection;
    }

    private void execSql(Connection connection, String sql) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(sql);
        ResultSetMetaData metadata = rs.getMetaData();
        int columnCount = metadata.getColumnCount();
        for (int i = 1; i < columnCount + 1; i++) {
            System.out.print(metadata.getColumnName(i));
            System.out.print("\t");
        }
        System.out.println();
        while (rs.next()) {
            for (int i = 1; i < columnCount + 1; i++) {
                System.out.print(rs.getObject(i));
                System.out.print("\t");
            }
            System.out.println();
        }
        rs.close();
        statement.close();
    }

    public void exec() throws SQLException, ClassNotFoundException {
        List<String> querys = getQuerys();
        Connection connection = getConnection();
        Connection conn = addSchema(connection, getName());
        for (String query : querys) {
            System.out.println("exec sql: " + query + ", result: ");
            execSql(conn, query);
        }
        conn.close();
        connection.close();
    }
}
