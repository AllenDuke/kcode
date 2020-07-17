package com.kuaishou.kcode;

/**
 * @author 杜科
 * @description
 * @contact AllenDuke@163.com
 * @date 2020/6/22
 */
public class Utils {

    //将num转换为截断保留2位小数的百分数字符串 0<=num<=1
    static ThreadLocal<SDS> sdsThreadLocal=new ThreadLocal<>();//会多个线程调用
    public static String doubleToPersentage(double num) {
        if (num == 1) return "100.00%";
        if(num==0) return ".00%";
        SDS sb = sdsThreadLocal.get();//已预设值，减少判断是否为空这一运算
        sb.clear();

        //乘法耗时

        num*=10;
        int n= (int) num;
        sb.append((char) (n+'0'));
        num-=n;
        num*=10;
        n= (int) num;
        sb.append((char) (n+'0'));
        sb.append('.');
        num-=n;
        num*=10;
        n= (int) num;
        sb.append((char) (n+'0'));
        num-=n;
        num*=10;
        n= (int) num;
        sb.append((char) (n+'0'));
        sb.append('%');
        String re = sb.toString();
        return re;
    }

    //将表示日期的date字符串加上1分钟后返回
    static SDS sdsDate=new SDS();//只有主线程使用
    public static String dateIncreaseMinute(String date) {
        sdsDate.clear();
        sdsDate.append(date);
        if (sdsDate.charAt(15) != '9') {//最后一位数字不是9
            sdsDate.setCharAt(15, (char) (sdsDate.charAt(15) + 1));
            return sdsDate.toString();
        }
        sdsDate.setCharAt(15, '0');//进位
        if (sdsDate.charAt(14) != '5') {
            sdsDate.setCharAt(14, (char) (sdsDate.charAt(14) + 1));
            return sdsDate.toString();
        }
        sdsDate.setCharAt(14, '0');//进位
        if (sdsDate.charAt(12) == '9') {
            sdsDate.setCharAt(12, '0');
            sdsDate.setCharAt(11, (char) (sdsDate.charAt(11) + 1));
            return sdsDate.toString();
        }
        if (sdsDate.charAt(11) == '2' && sdsDate.charAt(12) == '3') {//跨天
            sdsDate.setCharAt(11,'0');
            sdsDate.setCharAt(12,'0');
            sdsDate.setCharAt(9, (char) (sdsDate.charAt(9)+1));//假定不会是9
            return sdsDate.toString();
        }
        sdsDate.setCharAt(12, (char) (sdsDate.charAt(12) + 1));
        return sdsDate.toString();
    }
}
