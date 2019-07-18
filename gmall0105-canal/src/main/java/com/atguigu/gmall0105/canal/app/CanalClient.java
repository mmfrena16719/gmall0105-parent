package com.atguigu.gmall0105.canal.app;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * @Description
 * @Author Zhang Yanting Email:996739940@qq.com
 * @Date 2019/7/1 19:08
 * @Version 1.0
 */
public class CanalClient {
    public static void watch(String hostname, int port, String destination, String tables) {
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(
                new InetSocketAddress(hostname, port), destination, "", "");

        while (true) {
            //建立连接，注册要监控的表
            canalConnector.connect();
            canalConnector.subscribe(tables);
            Message message = canalConnector.get(100);

            int size = message.getEntries().size();

            if (size == 0) {
                System.out.println("没有数据，休息一会");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                //数据的基本单位是entry集合中的每一条entry
                for(CanalEntry.Entry entry : message.getEntries()){
                    if(entry.getEntryType().equals(CanalEntry.EntryType.ROWDATA)){
                        CanalEntry.RowChange rowChange = null;
                        try{
                            rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                        }catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }

                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();//行集
                        String tableName = entry.getHeader().getTableName();//表名
                        CanalEntry.EventType eventType = rowChange.getEventType();// 操作行为 insert update delete
                        CanalHandler canalHandler = new CanalHandler(rowDatasList, tableName, eventType);
                        canalHandler.handle();

                    }
                }
            }
        }

    }
}
