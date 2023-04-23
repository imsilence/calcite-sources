package com.silence.calcite.sources.v5;

import org.apache.calcite.config.Lex;

import java.sql.*;
import java.util.Properties;

public class SchemaType {
    public static Connection getConnection() throws ClassNotFoundException, SQLException {
        Class.forName("org.apache.calcite.jdbc.Driver");
        Properties properties = new Properties();
        properties.setProperty("lex", Lex.MYSQL.name());
        properties.setProperty("schemaType", "JDBC");
        properties.setProperty("schema.jdbcUrl", "jdbc:mariadb://localhost:3306/iam");
        properties.setProperty("schema.jdbcUser", "root");
        properties.setProperty("schema.jdbcPassword", "881019");
        return DriverManager.getConnection("jdbc:calcite:", properties);
    }

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        Connection connection = getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select name from users");
        while (resultSet.next()) {
            System.out.println(resultSet.getObject(1));
        }
        resultSet.close();
        connection.close();
    }
}
