package com.bourgeoisie.hive.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class HiveJdbc {
    public static void main(String[] args) throws Exception {
        //①加载驱动
        //Class.forName("org.apache.hive.jdbc.HiveDriver");
        //②创建连接
        Connection connection = DriverManager.getConnection("jdbc:hive2://slave02:10000", "hive", "hive");
        // ③准备SQL
//        String sql = "select * from cs.student";
        String sql = "show databases";
        // ④预编译sql
        PreparedStatement ps = connection.prepareStatement(sql);
        // ⑤执行sql
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
//            System.out.println("name:" + resultSet.getString("name") + "---->id:" +
//                    resultSet.getInt("id"));
            String database = resultSet.getString(1);
            System.out.println(database);
            if (database.equalsIgnoreCase("sys") || database.equalsIgnoreCase("information_schema")) {
                continue;
            }
            String sql2 = "show tables in " + database;
            PreparedStatement preparedStatement = connection.prepareStatement(sql2);
            ResultSet resultSet2 = preparedStatement.executeQuery();
            System.out.println("数据库" + database + "下的表");
            while (resultSet2.next()) {
                String table = resultSet2.getString(1);
                System.out.println(table);
            }
        }
    }
}
