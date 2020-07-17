package com.kuaishou.kcode;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author 杜科
 * @description 线程安全的日期格式化
 * @contact AllenDuke@163.com
 * @date 2020/7/14
 */
public class ConcurrentDateFormat {

    //这是线程不安全的 巨坑
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");//hh 为12小时制

    // 采用加锁的方式，防止同步问题
    public static String format(Date date){
        synchronized (dateFormat){
            return dateFormat.format(date);
        }
    }

    public static Date parse(String date) throws ParseException {
        synchronized (dateFormat){
            return dateFormat.parse(date);
        }
    }
}
