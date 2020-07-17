package com.kuaishou.kcode;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Method;

/**
 * @author kcode
 * Created on 2020-05-20
 */
public class KcodeMain1 {

    public static void main(String[] args) throws Exception {
        long start=System.currentTimeMillis() / 1000;
        // "demo.data" 是你从网盘上下载的测试数据，这里直接填你的本地绝对路径
        InputStream fileInputStream = new FileInputStream("D:\\Chrome\\Download\\warmup-test.data");
        Class<?> clazz = Class.forName("com.kuaishou.kcode.KcodeQuestion1");
        Object instance = clazz.newInstance();
        Method prepareMethod = clazz.getMethod("prepare", InputStream.class);
        Method getResultMethod = clazz.getMethod("getResult", Long.class, String.class);
        // 调用prepare()方法准备数据
        prepareMethod.invoke(instance, fileInputStream);

        // 验证正确性
        // "result.data" 是你从网盘上下载的结果数据，这里直接填你的本地绝对路径
        BufferedReader reader = new BufferedReader(new InputStreamReader(
                new FileInputStream("D:\\Chrome\\Download\\result-test.data")));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] split = line.split("\\|");
            String[] keys = split[0].split(",");
            // 调用getResult()方法
            Object result = getResultMethod.invoke(instance, new Long(keys[0]), keys[1]);
            if (!split[1].equals(result)) {
                System.out.println(keys[0]+" "+keys[1]+" fail 理论："+split[1]+"实际："+result);
            }
//            else System.out.println("success");
        }
        System.out.println(System.currentTimeMillis() / 1000 - start);
    }
}