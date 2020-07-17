package com.kuaishou.kcode;

import java.text.NumberFormat;
import java.text.ParseException;

/**
 * @author 杜科
 * @description 线程安全的数字格式化
 * @contact AllenDuke@163.com
 * @date 2020/7/14
 */
public class ConcurrentNumberFormat {

    //用于将百分数转换为double 这是线程不安全的 巨坑
    private static final NumberFormat numberFormat = NumberFormat.getPercentInstance();

    // 采用加锁的方式，防止同步问题
    public static double toDouble(String value) throws ParseException {
        synchronized (numberFormat){
            return numberFormat.parse(value).doubleValue();
        }
    }
}
