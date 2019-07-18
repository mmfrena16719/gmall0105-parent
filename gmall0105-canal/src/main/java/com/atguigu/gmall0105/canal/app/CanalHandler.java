package com.atguigu.gmall0105.canal.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.atguigu.gmall.constant.GmallConstants;
import com.atguigu.gmall0105.canal.util.MyKafkaSender;

import java.util.List;
import java.util.Random;

import java.util.List;

/**
 * @Description
 * @Author Zhang Yanting Email:996739940@qq.com
 * @Date 2019/7/1 19:08
 * @Version 1.0
 */
public class CanalHandler {

    private List<CanalEntry.RowData> rowDataList;
    String tableName;
    CanalEntry.EventType eventType;

    //构造器
    public CanalHandler(List<CanalEntry.RowData> rowDataList, String tableName, CanalEntry.EventType eventType) {
        this.rowDataList = rowDataList;
        this.tableName = tableName;
        this.eventType = eventType;
    }

    //处理器，监视对不同表格的不同操作，从而做出不同的行为
    public void handle() {
        if (eventType.equals(CanalEntry.EventType.INSERT) && tableName.equals("order_info")) {
            sendRowList2Kafka(GmallConstants.KAFKA_TOPIC_ORDER);
        } else if ((eventType.equals(CanalEntry.EventType.INSERT) || eventType.equals(CanalEntry.EventType.UPDATE)) && tableName.equals("user_info")) {
            sendRowList2Kafka(GmallConstants.KAFKA_TOPIC_USER);
        } else if (eventType.equals(CanalEntry.EventType.INSERT) && tableName.equals("order_detail")) {
            sendRowList2Kafka(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL);
        }
    }

    private void sendRowList2Kafka(String kafkaTopic) {

        for (CanalEntry.RowData rowData : rowDataList) {
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
            JSONObject jsonObject = new JSONObject();
            for (CanalEntry.Column column : afterColumnsList) {
                System.out.println(column.getName() + "--->" + column.getValue());
                jsonObject.put(column.getName(), column.getValue());
            }
            try {
                Thread.sleep(new Random().nextInt(5) * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            MyKafkaSender.send(kafkaTopic, jsonObject.toJSONString());
        }
    }


}
