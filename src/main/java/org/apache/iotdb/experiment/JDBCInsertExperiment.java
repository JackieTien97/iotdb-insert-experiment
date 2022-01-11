package org.apache.iotdb.experiment;

import org.apache.iotdb.jdbc.IoTDBSQLException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.experiment.Constant.BATCH_INSERT_SIZE;
import static org.apache.iotdb.experiment.Constant.TOTAL_INSERT_ROW_COUNT;

public class JDBCInsertExperiment {

    public static void main(String[] args) throws ClassNotFoundException {

        Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
        try (Connection connection =
                     DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
             Statement statement = connection.createStatement()) {
            // 记录耗时
            long startTime = System.currentTimeMillis();
            jdbcInsert(statement);
            long endTime = System.currentTimeMillis();
            System.out.println("JDBC insert " + TOTAL_INSERT_ROW_COUNT + " rows cost: " + (endTime - startTime) + "ms.");

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    private static void jdbcInsert(Statement statement) throws SQLException {
        // 设置设备名字，设备下面的传感器名字，各个传感器的类型
        statement.execute("SET STORAGE GROUP TO root.ln");
        statement.execute(
                "CREATE TIMESERIES " +
                        "root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=RLE, COMPRESSOR=SNAPPY");
        statement.execute(
                "CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=DOUBLE, ENCODING=RLE, COMPRESSOR=SNAPPY");
        statement.execute(
                "CREATE TIMESERIES root.ln.wf01.wt01.speed WITH DATATYPE=INT64, ENCODING=RLE, COMPRESSOR=SNAPPY");

        // 以当前时间戳作为插入的起始时间戳
        long timestamp = System.currentTimeMillis();
        int row = 0;
        while (row < TOTAL_INSERT_ROW_COUNT) {
            row++;
            statement.addBatch(prepareInsertStatment(timestamp + row, row));
            if (row % BATCH_INSERT_SIZE == 0) {
                statement.executeBatch();
                statement.clearBatch();
                System.out.println("已经插入了：" + row + "行数据");
            }
        }

        // 插入剩余不足 BATCH_INSERT_SIZE的数据
        if (row != 0 && (row % BATCH_INSERT_SIZE) != 0) {
            statement.executeBatch();
            statement.clearBatch();
        }
    }

    private static String prepareInsertStatment(long time, long row) {
        return "insert into root.ln.wf01.wt01(timestamp, status, temperature, speed) values("
                + time
                + ","
                + ((row & 1) == 0)
                + ","
                + ((double) row)
                + ","
                + row
                + ")";
    }
}
