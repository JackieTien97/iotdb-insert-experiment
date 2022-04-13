package org.apache.iotdb.experiment;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;

public class SessionGroupByInterval {

    public static void main(String[] args) throws InterruptedException, IoTDBConnectionException, StatementExecutionException {

        Session session = new Session("127.0.0.1", 6667, "root", "root");
        session.setFetchSize(10000);
        session.open(false);
        // 记录耗时
        long startTime = System.currentTimeMillis();

        SessionDataSet sessionDataSet = session.executeQueryStatement(args[0]);
        while (sessionDataSet.hasNext()) {
            sessionDataSet.next();
        }

        long endTime = System.currentTimeMillis();

        System.out.println("down sampling 15,000,000,000 rows to 1,000,000 rows cost: " + (endTime - startTime) + "ms.");


    }
}
