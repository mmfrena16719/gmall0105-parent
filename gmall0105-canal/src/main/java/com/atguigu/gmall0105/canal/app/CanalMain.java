package com.atguigu.gmall0105.canal.app;

/**
 * @Description
 * @Author Zhang Yanting Email:996739940@qq.com
 * @Date 2019/7/1 19:08
 * @Version 1.0
 */
public class CanalMain {
    public static void main(String[] args) {
        CanalClient.watch("hadoop102",11111,"example","gmall0105.*");
//        CanalClient.watch("hadoop102",11111,"example2","gmall0105.user_info");
    }
}
