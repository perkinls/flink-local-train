package com.lp.java.demo.commons.utils;

import java.util.Random;

/**
 * @author li.pan
 * @version 1.0.0
 * @title 随机生成Double温度值
 * @createTime 2021年03月08日 21:54:00
 */
public class DoubleUtils {

    /**
     * 生成max到min范围的浮点数
     */
    public static double nextDouble(final double min, final double max) {
        return formatRate(String.valueOf(min + ((max - min) * new Random().nextDouble())));
    }

    /**
     * 保留小数点后两位小数
     *
     * @param rateStr xx.xxxxx
     * @return result
     */
    public static Double formatRate(String rateStr) {
        if (rateStr.indexOf(".") != -1) {
            // 获取小数点的位置
            int num = 0;
            num = rateStr.indexOf(".");

            String dianAfter = rateStr.substring(0, num + 1);
            String afterData = rateStr.replace(dianAfter, "");

            return Double.valueOf(rateStr.substring(0, num) + "." + afterData.substring(0, 2));
        } else {
            if (rateStr == "1") {
                return Double.valueOf("100");
            } else {
                return Double.valueOf(rateStr);
            }
        }
    }

//    public static void main(String[] args) {
//        System.out.println(nextDouble(60,100));
//    }
}
