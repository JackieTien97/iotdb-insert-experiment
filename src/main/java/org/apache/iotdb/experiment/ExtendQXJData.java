/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.experiment;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.iotdb.experiment.Constant.BATCH_INSERT_SIZE;

public class ExtendQXJData {

  public static void main(String[] args) throws IoTDBConnectionException, StatementExecutionException {

    Session session1 = new Session("127.0.0.1", 6667, "root", "root");
    session1.setFetchSize(10000);
    session1.open(false);
    int port2 = Integer.parseInt(args[0]);
    Session session2 = new Session("127.0.0.1", port2, "root", "root");
    session2.setFetchSize(10000);
    session2.open(false);
    String device = args[1];
    SessionDataSet dataSet = session1.executeQueryStatement("show timeseries " + device + ".*");
    List<MeasurementSchema> schemaList = new ArrayList<>();
    while (dataSet.hasNext()) {
      RowRecord rowRecord = dataSet.next();
      if (rowRecord == null) {
        throw new IllegalArgumentException("Device: " + device + " doesn't have any sensor");
      }
      List<Field> fields = rowRecord.getFields();
      String[] sensorNameArray = fields.get(0).getStringValue().split("\\.");
      String sensorName = sensorNameArray[sensorNameArray.length - 1];
      System.out.println(sensorName);
      String type = fields.get(3).getStringValue();
      System.out.println(type);
      TSDataType dataType = TSDataType.valueOf(type);
      schemaList.add(new MeasurementSchema(sensorName, dataType));
    }
    dataSet.closeOperationHandle();
    dataSet = null;

    SessionDataSet minTimeDataset = session1.executeQueryStatement("select min_time(*) from " + device);
    long startTime = Long.MAX_VALUE;
    while (minTimeDataset.hasNext()) {
      RowRecord rowRecord = minTimeDataset.next();
      List<Field> fields = rowRecord.getFields();
      if (fields.size() < 1) {
        throw new IllegalArgumentException("Device: " + device + " doesn't have any sensor");
      }
      for (Field field : fields) {
        startTime = Math.min(startTime, field.getLongV());
      }
    }
    minTimeDataset.closeOperationHandle();
    minTimeDataset = null;

    if (startTime == Long.MAX_VALUE) {
      throw new IllegalArgumentException("Device: " + device + " doesn't have any sensor");
    }

    SessionDataSet maxTimeDataset = session1.executeQueryStatement("select max_time(*) from " + device);
    long endTime = Long.MIN_VALUE;
    while (maxTimeDataset.hasNext()) {
      RowRecord rowRecord = maxTimeDataset.next();
      List<Field> fields = rowRecord.getFields();
      if (fields.size() < 1) {
        throw new IllegalArgumentException("Device: " + device + " doesn't have any sensor");
      }
      for (Field field : fields) {
        endTime = Math.max(endTime, field.getLongV());
      }
    }
    maxTimeDataset.closeOperationHandle();
    maxTimeDataset = null;

    if (endTime == Long.MIN_VALUE) {
      throw new IllegalArgumentException("Device: " + device + " doesn't have any sensor");
    }


    Tablet tablet = new Tablet(device, schemaList, BATCH_INSERT_SIZE);
    tablet.initBitMaps();

    long delta = endTime - startTime + 1;
    int count = 1;
    long finalEndTime = startTime + 365L * 10 * 24 * 60 * 60 * 1000;
    long timestamp = startTime;
    long rowCount = 0;
    String querySql = "select " + schemaList.stream().map(MeasurementSchema::getMeasurementId).collect(Collectors.joining(",")) + " from " + device;

    while (timestamp <= finalEndTime) {
      SessionDataSet deviceData = session1.executeQueryStatement(querySql);
      while (deviceData.hasNext()) {
        int rowIndex = tablet.rowSize++;
        RowRecord rowRecord = deviceData.next();
        List<Field> fields = rowRecord.getFields();
        timestamp = rowRecord.getTimestamp() + delta * count;
        tablet.addTimestamp(rowIndex, timestamp);
        for (int i = 0; i < schemaList.size(); i++) {
          if (fields.get(i) == null || fields.get(i).getDataType() == null) {
            tablet.bitMaps[i].mark(rowIndex);
          } else {
            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, fields.get(i).getObjectValue(schemaList.get(i).getType()));
          }
        }
        rowCount++;
        if (tablet.rowSize == tablet.getMaxRowNumber()) {
          session2.insertTablet(tablet, true);
          tablet.reset();
          System.out.println("已经插入了：" + rowCount + "行数据");
        }
      }
      deviceData.closeOperationHandle();
      count++;
    }

    // 插入剩余不足 BATCH_INSERT_SIZE的数据
    if (tablet.rowSize != 0) {
      session2.insertTablet(tablet, true);
      tablet.reset();
    }
  }
}
