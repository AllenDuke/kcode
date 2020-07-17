package com.kuaishou.kcode;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author kcode
 * Created on 2020-05-20
 */
public class KcodeQuestion {

    /**
     * key为秒，如1589761895
     * io也负责计算时把key改为Thread，各个线程解析完一秒的数据后开始计算
     */
    private Map<Thread, Map<String, List<Integer>>> threadMap = new ConcurrentHashMap<>();

    //key为秒
    private Map<Long, Map<String, String>> resultMap = new ConcurrentHashMap(4200);

    //读取文件的线程数
    private int readThreadNum = 4;//10线程会出问题


    /**
     * prepare() 方法用来接受输入数据集，数据集格式参考README.md
     *
     * @param inputStream
     */
    public void prepare(InputStream inputStream) throws IOException, InterruptedException {
        parseInNIO(inputStream);
        threadMap = null;
//        timeMap=null;
    }

    /**
     * @param inputStream
     * @description: 强转inputStream为FileInputStream，取出channel，利用NIO来解析
     * 首先据io线程数，划定每个线程要处理的理论区间大小，而后在这个区间上确定出真正的区间。
     * 实际区间的要求：每个线程要完整处理某一秒的数据，因此理论区间的右边应该包括下一秒的数据，这样的变动用inputStream来确定，因此
     * 要十分注意inputStream中的position。
     * 最后这个区间Buffer使用MappedByteBuffer，利用内存映射减少复制。
     * @return: void
     * @author: 杜科
     * @date: 2020/6/6
     */
    private void parseInNIO(InputStream inputStream) throws IOException, InterruptedException {
        FileInputStream fileInputStream = (FileInputStream) inputStream;
        FileChannel channel = fileInputStream.getChannel();
        List<Thread> threads = new ArrayList<>(readThreadNum);
        long begin = 0;
        long len = channel.size() / readThreadNum;//每个线程理论处理大小
        for (int i = 0; i < readThreadNum; i++) {//分割文件，多个线程并行读
            long end = (begin + len - 1) < channel.size() - 1 ? (begin + len - 1) : channel.size() - 1;

            long end2 = calRealEndInNIO(end + 1, channel);
            end = end2;
            final long tb = begin;
            final long te = end;
            //当前线程要处理的实际区间是begin~end
            Thread thread = new Thread(() -> {
                long cur = tb;
                long tail = te;

//                System.out.println(Thread.currentThread().getName() + "处理范围：" + cur + "-" + tail);
                try {
                    while (tail - cur + 1 >= 128 * 1024 * 1024) {//在当前处理范围内，每32MB建立一个内存映射
                        //理论上在机器物理内存允许的基础上，应该尽可能接近Integer.MAX_VALUE
                        //todo 应该1个线程读的，多个线程对这个buffer进行处理，多个线程读并没有效果，但是我直接分成每个线程一次差不多1G大小也没有效果
                        MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, cur,
                                128* 1024 * 1024);
                        cur = handleBuf(cur, mappedByteBuffer, tail);//更新cur
                    }
                    if (cur <= tail) {
                        MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, cur,
                                tail - cur + 1);
                        cur = handleBuf(cur, mappedByteBuffer, tail);
//                        System.out.println("thread-" + Thread.currentThread().getName() + " reach: " + cur);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            threads.add(thread);
            thread.setName("io thread-" + i);
            threadMap.put(thread, new HashMap<>(69));
            thread.start();
            begin = end + 1;
        }
        for (Thread thread : threads) {
            thread.join();
        }
    }

    /**
     * @param cur        在文件中的position
     * @param byteBuffer
     * @param tail       区间末尾
     * @description: 对byteBuffer进行按行解析，因为不确定可以完整解析，所以在解析前记录行开始位置，完整解析一行后更新位置，
     * 如果不能完整解析一行，返回行开始位置，作下一个buffer的map开始位置
     * @return: long 指向下一个要处理的字节
     * @author: 杜科
     * @date: 2020/6/6
     */
    private long handleBuf(long cur, ByteBuffer byteBuffer, long tail) {
        String curSecond = null;
        while (true) {//优化了判断条件
            long begin = cur + byteBuffer.position();//标记行开始时，channel position所在
//            System.out.println(begin);
            try {//开始处理一行数据
                //截取秒
                StringBuilder invokeTime = new StringBuilder(10);
                //使用SDS来减少对象的产生居然反而变慢，难道没有发生垃圾回收吗
                int i = 0;
                while (i < 10) {
                    invokeTime.append((char) byteBuffer.get());
                    i++;
                }

                if (curSecond == null) curSecond = invokeTime.toString();
                else if (curSecond.compareTo(invokeTime.toString()) < 0) {//如果已经完整解析一秒
                    calCurSecond(curSecond, Thread.currentThread());//结算上一秒
//                    calCurSecondByPool(curSecond,Thread.currentThread());
                    curSecond = invokeTime.toString();
                }
                //跳过毫秒
                for (int j = 0; j < 3; j++) byteBuffer.get();
                //跳过','
                byteBuffer.get();
                //截取方法名
                StringBuilder methodName = new StringBuilder();
                char ch = (char) byteBuffer.get();
                while (ch != ',') {
                    methodName.append(ch);
                    ch = (char) byteBuffer.get();
                }
                //截取调用耗时
                int sum = 0;
                /**
                 * 减少强制类型转换，应该作用不大，因为在编译阶段会优化，而且在栈上时，char与int同样占4个字节，char被看作int.
                 */
                int c=byteBuffer.get();
                while (c != '\n') {
                    sum = sum * 10 + (c-48);
                    c = byteBuffer.get();
                }
//                ch= (char) byteBuffer.get();
//                while (ch != '\n') {
//                    sum=sum*10+(ch-'0');
//                    ch= (char) byteBuffer.get();
//                }
                Map<String, List<Integer>> listMap = threadMap.get(Thread.currentThread());
//                Map<String, List<Integer>> listMap = timeMap.get(curSecond);
//                if(listMap==null){
//                    listMap=new ConcurrentHashMap<>();
//                    timeMap.put(curSecond,timeMap);
//                }
                List<Integer> list = listMap.get(methodName.toString());
                if (list == null) {
                    list = new ArrayList<>();
                    listMap.put(methodName.toString(), list);
                }
                list.add(sum);

                //System.out.println(invokeTime.toString()+","+ms.toString()+","+methodName+","+sum);

            } catch (Exception e) {
//                e.printStackTrace();
//                System.out.println("该行不能完整解析，回退到" + begin);
                cur += byteBuffer.capacity();
                if (cur > tail)//到达区间末尾时，计算最后一秒
                    calCurSecond(curSecond, Thread.currentThread());
                return begin;
            }
        }
    }

    private long calRealEndInNIO(long cur, FileChannel channel) throws IOException {
        String curSecond = null;
        long len = 5 * 1024 * 1024;
        long size = channel.size() - cur > len ? len : channel.size() - cur;
        MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, cur, size);
        //计算最近的换行符
        while (buffer.position() < buffer.capacity() && buffer.get() != '\n') ;
        while (buffer.position() < buffer.capacity()) {//尝试在接下来的5 MB内存中找
            //这里偷懒了，偷懒的前提是保证在5MB内找到
            //截取秒
            StringBuilder invokeTime = new StringBuilder();
            int count = 10;
            while (count > 0) {
                char ch = (char) buffer.get();
                invokeTime.append(ch);
                count--;
            }
            if (curSecond == null) curSecond = invokeTime.toString();
            else if (curSecond.compareTo(invokeTime.toString()) < 0) {//寻找到完整的一秒
//                System.out.println(curSecond + " " + invokeTime.toString());
                return cur + buffer.position() - 10 - 1;//注意
            }
            while (buffer.get() != '\n') ;
        }
        return cur - 1;
    }

    //对某一秒的计算任务，这一秒任务的计算应该是很快的
    private void calCurSecond(String curSecond, Thread thread) {
        Map<String, List<Integer>> listMap = threadMap.get(thread);//取出该线程要处理的某一秒
//        Map<String, List<Integer>> listMap = timeMap.get(curSecond);//取出该线程要处理的某一秒
        Long cur = Long.parseLong(curSecond);//减少toString转换的运算
        Map<String, String> methodMap = resultMap.get(cur);
        if (methodMap == null) {
            methodMap = new HashMap<>();
            resultMap.put(cur, methodMap);
        }
        Iterator<String> iterator1 = listMap.keySet().iterator();
        while (iterator1.hasNext()) {
            String methodName = iterator1.next();
            List<Integer> list = listMap.get(methodName);
            Collections.sort(list);
            int qps = list.size();
            int i99 = (int) (list.size() * 0.99);//使用类库会有不一样的效果吗
            if (list.size() % 100 != 0) i99++;
            int p99 = list.get(i99 - 1);
            int i50 = list.size() >> 1;
            if ((list.size() & 1) != 0) i50++;
            int p50 = list.get(i50 - 1);
            int sum = 0;
            for (int i : list) {
                sum += i;
            }
            int avg = 0;
            if (sum % list.size() != 0) avg = sum / list.size() + 1;
            else avg = sum / list.size();
            String result = qps + "," + p99 + "," + p50 + "," + avg + "," + list.get(list.size() - 1);
            methodMap.put(methodName, result);
//            System.out.println(curSecond + " " + methodName + " " + result);
        }
//        timeMap.remove(curSecond);
        listMap.clear();

    }


    /**
     * getResult() 方法是由kcode评测系统调用，是评测程序正确性的一部分，请按照题目要求返回正确数据
     * 输入格式和输出格式参考 README.md
     *
     * @param timestamp  秒级时间戳
     * @param methodName 方法名称
     */
    public String getResult(Long timestamp, String methodName) {//减少toString转换的运算
//        if(!resultMap.containsKey(timestamp.toString())) return "";
        String result = resultMap.get(timestamp).get(methodName);//尝试只使用1个map
        return result;
    }

}
