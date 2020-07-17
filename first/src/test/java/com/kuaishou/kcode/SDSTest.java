package com.kuaishou.kcode;

/**
 * @author 杜科
 * @description
 * @contact AllenDuke@163.com
 * @date 2020/6/24
 */
public class SDSTest {

    static int count=10000000;

    public static void main(String[] args) {
        long start=System.currentTimeMillis();
        String date="2020-06-18 11:20";
        for (int i = 0; i < count; i++) {
//            testSB1(date);
            testSB2(0.335435);
        }
        System.out.println(System.currentTimeMillis() - start);
        start=System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
//            testSB1(date);
            testSDS2(0.335435);
        }
        System.out.println(System.currentTimeMillis() - start);
    }

    public static String testSB1(String date){
        StringBuilder sb=new StringBuilder(date);
        if (sb.charAt(15) != '9') {//最后一位数字不是9
            sb.setCharAt(15, (char) (sb.charAt(15) + 1));
            return sb.toString();
        }
        sb.setCharAt(15, '0');//进位
        if (sb.charAt(14) != '5') {
            sb.setCharAt(14, (char) (sb.charAt(14) + 1));
            return sb.toString();
        }
        sb.setCharAt(14, '0');//进位
        if (sb.charAt(12) == '9') {
            sb.setCharAt(12, '0');
            sb.setCharAt(11, (char) (sb.charAt(11) + 1));
            return sb.toString();
        }
        if (sb.charAt(11) == '2' && sb.charAt(12) == '3') {//跨天
            sb.setCharAt(11,'0');
            sb.setCharAt(12,'0');
            sb.setCharAt(9, (char) (sb.charAt(9)+1));//假定不会是9
            return sb.toString();
        }
        sb.setCharAt(12, (char) (sb.charAt(12) + 1));
        return sb.toString();
    }

    public static String testSB2(double num){
        if (num == 1) return "100.00%";
        if(num==0) return ".00%";
        StringBuilder sb=new StringBuilder(6);
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

    //count越大性能越好好，但在千万级上都是1秒
    static SDS sdsDate=new SDS();
    public static String testSDS1(String date){
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

    //性能比StirngBuilder稍好， count越大性能相差越少
    static ThreadLocal<SDS> sdsThreadLocal=new ThreadLocal<>();
    public static String testSDS2(double num){
        if (num == 1) return "100.00%";
        if(num==0) return ".00%";
        SDS sb = sdsThreadLocal.get();
        if(sb==null){
            sb=new SDS(6);
            sdsThreadLocal.set(sb);
        }
        sb.clear();

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

}
