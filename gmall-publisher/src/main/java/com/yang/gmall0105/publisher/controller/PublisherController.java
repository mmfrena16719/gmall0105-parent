package com.yang.gmall0105.publisher.controller;


import com.alibaba.fastjson.JSON;
import com.yang.gmall0105.publisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.apache.phoenix.util.DateUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.print.attribute.standard.PrinterLocation;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
/***
 * @Author Zhang Yanting
 * @Description 生产数据的接口
 * @Date 14:56 2019/7/1
 * $Param 
 * $return 
 */
@RestController
public class PublisherController {


    @Autowired
    PublisherService publisherService;



    @GetMapping("realtime-total")
    public String getRealTotal(@RequestParam("date") String date){
        Long dautoTal = publisherService.getDauTotal(date);
        List toTalList  = new ArrayList();
        Map dauMap = new HashMap();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        dauMap.put("value",dautoTal);
        toTalList.add(dauMap);
        Map newMidMap = new HashMap();
        newMidMap.put("id","newMid");
        newMidMap.put("name","新增设备");
        newMidMap.put("value",233);
        toTalList.add(newMidMap);

        return JSON.toJSONString(toTalList);

    }
    @GetMapping("realtime-hour")
    public String getRealtimeHour(@RequestParam("id") String id,@RequestParam("date") String date){

        if("dau".equals(id)){
            Map hourMap = new HashMap();
            Map dauhourTMap = publisherService.getDauHour(date);
            String ydate = getYdate(date);
            Map dauHourYMap = publisherService.getDauHour(ydate);

            hourMap.put("yesterday",dauHourYMap);
            hourMap.put("today",dauhourTMap);
            return JSON.toJSONString(hourMap);
        }


        return null;

    }
    public String getYdate(String date){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String ydate = null;

        try {
            Date tdate = simpleDateFormat.parse(date);
            Date yesdate = DateUtils.addDays(tdate,-1);
            ydate = simpleDateFormat.format(yesdate);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return ydate;
    }

}
