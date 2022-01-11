package org.apache.iotdb.experiment;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.experiment.Constant.BATCH_INSERT_SIZE;
import static org.apache.iotdb.experiment.Constant.TOTAL_INSERT_ROW_COUNT;

public class SessionInsertExperiment {

    public static void main(String[] args) {
        Session session = new Session("127.0.0.1", 6667, "root", "root");
        session.setFetchSize(2048);

        try {
            session.open(false);
            // 记录耗时
            long startTime = System.currentTimeMillis();
            insertTablet(session);
            long endTime = System.currentTimeMillis();

            System.out.println("Session insert " + TOTAL_INSERT_ROW_COUNT + " rows cost: " + (endTime - startTime) + "ms.");

            startTime = System.currentTimeMillis();
            SessionDataSet sessionDataSet = session.executeQueryStatement("select temperature from root.ln.wf01.wt01");
            while (sessionDataSet.hasNext()) {
                sessionDataSet.next();
            }
            endTime = System.currentTimeMillis();

            System.out.println("Session query " + TOTAL_INSERT_ROW_COUNT + " rows cost: " + (endTime - startTime) + "ms.");

        } catch (IoTDBConnectionException | StatementExecutionException e) {
            e.printStackTrace();
        }
    }

    /**
     * 使用Session.insertTablet接口插入某一个设备的数据
     */
    private static void insertTablet(Session session) throws IoTDBConnectionException, StatementExecutionException {
        /*
         * 一个Tablet例子:
         * deviceID: root.ln.wf01.wt01
         * time status, temperature, speed
         * 1    true        1.0       1
         * 2    false       2.0       2
         * 3    true        3.0       3
         */
        // 设置设备名字，设备下面的传感器名字，各个传感器的类型
        List<MeasurementSchema> schemaList = new ArrayList<>();
        schemaList.add(new MeasurementSchema("status", TSDataType.BOOLEAN));
        schemaList.add(new MeasurementSchema("temperature", TSDataType.DOUBLE));
        schemaList.add(new MeasurementSchema("speed", TSDataType.INT64));

        Tablet tablet = new Tablet("root.ln.wf01.wt01", schemaList, BATCH_INSERT_SIZE);


        // 以当前时间戳作为插入的起始时间戳
        long timestamp = System.currentTimeMillis();

        for (long row = 0; row < TOTAL_INSERT_ROW_COUNT; row++) {
            int rowIndex = tablet.rowSize++;
            tablet.addTimestamp(rowIndex, timestamp);
            // 随机生成数据
            tablet.addValue("status", rowIndex, (row & 1) == 0);
            tablet.addValue("temperature", rowIndex, (double) row);
            tablet.addValue("speed", rowIndex, row);

            if (tablet.rowSize == tablet.getMaxRowNumber()) {
                session.insertTablet(tablet);
                tablet.reset();
                System.out.println("已经插入了：" + (row + 1) + "行数据");
            }
            timestamp++;
        }

        // 插入剩余不足 BATCH_INSERT_SIZE的数据
        if (tablet.rowSize != 0) {
            session.insertTablet(tablet);
            tablet.reset();
        }
    }
}
