package com.atguigu.gmall0105.logger.controller;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Description
 * @Author Zhang Yanting Email:996739940@qq.com
 * @Date 2019/7/1 16:05
 * @Version 1.0
 */
public class TestDate {
    public static void main(String[] args) {
        System.out.println(new SimpleDateFormat("yyyy-MM-dd HH").format(
                new Date(System.currentTimeMillis())
        ));
    }
}
