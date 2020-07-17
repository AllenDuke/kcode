package com.kuaishou.kcode;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author kcode
 * Created on 2020-05-20
 */
public class KcodeQuestion1 {

    /**
     * key为秒，如1589761895
     * io也负责计算时把key改为Thread，各个线程解析完一秒的数据后开始计算
     */
    private Map<Thread, Map<String, List<Integer>>> threadMap = new ConcurrentHashMap<>();

    private Map<String, Map<String, List<Integer>>> timeMap = new ConcurrentHashMap<>();

    //key为秒
    private Map<String, Map<String, String>> resultMap = new ConcurrentHashMap<>(4200);

    Queue<String> queue1 = new LinkedBlockingQueue<>();

    Queue<String> queue2 = new LinkedBlockingQueue<>();

    Thread acounter1;

    Thread killer2;

    Thread main;

    String beginSecond = null;

    static boolean canStop1 = false;

    static boolean canStop2 = false;

    {
        main = Thread.currentThread();

        acounter1 = new Thread(() -> {
            while (!canStop1 || queue1.size() > 0) {
                String s1 = queue1.poll();
                if (s1 == null) continue;
                calCurSecond(s1, main);
            }
        });

        killer2 = new Thread(() -> {
            while (!canStop2 || queue2.size() > 0) {
                String s2 = queue2.poll();
                if (s2 == null) continue;
                String[] split = s2.split(",");
                split[0] = split[0].substring(0, 10);
                if (beginSecond == null) beginSecond = split[0];
                else if (beginSecond.compareTo(split[0]) < 0) {
                    queue1.add(beginSecond);
                    beginSecond = split[0];
                }
//                Map<String, List<Integer>> listMap = threadMap.get(main);

                Map<String, List<Integer>> listMap = timeMap.get(beginSecond);
                if (listMap == null) {
                    listMap = new HashMap<>(69);
                    timeMap.put(beginSecond, listMap);
                }

                List<Integer> list = listMap.get(split[1]);
                if (list == null) {
                    list = new ArrayList<>();
                    listMap.put(split[1], list);
                }
                list.add(new Integer(split[2]));

            }
            canStop1 = true;
        });
    }

    //读取文件的线程数
    private static int readThreadNum = 7;

    private static List<Thread> ioThreads = new ArrayList<>(readThreadNum);

    private static FileChannel channel;

    /**
     * prepare() 方法用来接受输入数据集，数据集格式参考README.md
     *
     * @param inputStream
     */
    public void prepare(InputStream inputStream) throws Exception {
        long start=System.currentTimeMillis() / 1000;
        parseInBIO(inputStream);
//        parseInNIO(inputStream);
//        killer2.join();
        threadMap = null;
//        SDSPool.stopClean();
        System.out.println(System.currentTimeMillis() / 1000 - start);
    }

    private void parseInBIO(InputStream inputStream) throws Exception {
        acounter1.start();
        killer2.start();
        long start = System.currentTimeMillis() / 1000;
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream), 1 * 1024 * 1024);
        String s;
        final Thread main = Thread.currentThread();
        threadMap.put(main, new HashMap<>(69));
        while ((s = bufferedReader.readLine()) != null) {
            queue2.add(s);
        }
        System.out.println("read completely " + (System.currentTimeMillis() / 1000 - start));
        canStop2=true;
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
        acounter1.start();
        FileInputStream fileInputStream = (FileInputStream) inputStream;
        channel = fileInputStream.getChannel();
        long begin = 0;
        long len = channel.size() / readThreadNum;//每个线程理论处理大小
        for (int i = 0; i < readThreadNum; i++) {//分割文件，多个线程并行读
            long end = (begin + len - 1) < channel.size() - 1 ? (begin + len - 1) : channel.size() - 1;
            long end2 = calRealEndInNIO(end + 1, channel);
            end = end2;
            final long tmpCur = begin;
            final long tmpTail = end;
            //当前线程要处理的实际区间是begin~end
            Thread ioThread = new Thread(() -> {
                long cur = tmpCur;
                long tail = tmpTail;
                //                System.out.println(Thread.currentThread().getName() + "处理范围：" + cur + "-" + tail);
                try {
                    while (tail - cur + 1 >= 4 * 1024 * 1024) {//在当前处理范围内，每1MB建立一个内存映射
                        //注意可用的直接内存,避免频繁发生操作系统页面置换
                        MappedByteBuffer byteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, cur,
                                4 * 1024 * 1024);

//                        ByteBuffer byteBuffer = ByteBuffer.allocate(4 * 1024 * 1024);
//                        channel.position(cur);
//                        channel.read(byteBuffer);
//                        byteBuffer.flip();

                        cur = handleBufByByte(cur, byteBuffer, tail);//更新cur
//                        System.out.println("处理完第 " + num++ + "块");
                    }
                    if (cur <= tail) {
                        MappedByteBuffer byteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, cur,
                                tail - cur + 1);

//                        ByteBuffer byteBuffer = ByteBuffer.allocate((int) (tail - cur + 1));
//                        channel.position(cur);
//                        channel.read(byteBuffer);
//                        byteBuffer.flip();

                        cur = handleBufByByte(cur, byteBuffer, tail);
//                        System.out.println("thread-" + Thread.currentThread().getName() + " reach: " + cur);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

            });
            ioThreads.add(ioThread);
            threadMap.put(ioThread, new HashMap<>(69));
            ioThread.setName("ioThread-" + i);
            ioThread.start();
            begin = end + 1;
        }
        for (int i = 0; i < readThreadNum; i++) {
            ioThreads.get(i).join();
        }
        acounter1.join();
        canStop1 = true;
    }

    private long handleBufByByte(long cur, ByteBuffer byteBuffer, long tail) {
        String curSecond = null;
        int i = 0;
        while (byteBuffer.position() < byteBuffer.capacity()) {
            long begin = cur + byteBuffer.position();//标记行开始时，channel position所在
//            System.out.println(begin);
            try {
                //开始处理一行数据
                //截取秒
                StringBuilder invokeTime = new StringBuilder(10);
                i = 0;
                while (i < 10) {
                    invokeTime.append((char) byteBuffer.get());
                    i++;
                }
                if (curSecond == null) curSecond = invokeTime.toString();
                else if (curSecond.compareTo(invokeTime.toString()) < 0) {//如果已经完整解析一秒
                    queue1.add(curSecond);
                    curSecond = invokeTime.toString();
                }
                //else SDSPool.release(invokeTime);
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
                ch = (char) byteBuffer.get();
                while (ch != '\n') {
                    sum = sum * 10 + (ch - '0');
                    ch = (char) byteBuffer.get();
                }
//                Map<SDS, List<Integer>> listMap = threadMap.get(Thread.currentThread());
                Map<String, List<Integer>> listMap = timeMap.get(curSecond);
                if (listMap == null) {
                    listMap = new HashMap<>(69);
                    timeMap.put(curSecond, listMap);
                }

                List<Integer> list = listMap.get(methodName.toString());
                if (list == null) {
                    list = new ArrayList<>();
                    listMap.put(methodName.toString(), list);
                }
                list.add(sum);
            } catch (Exception e) {
//                System.out.println(e.toString());
//                System.out.println("该行不能完整解析，回退到" + begin);
                return begin;
            }
        }
        cur += byteBuffer.capacity();
        if (cur > tail)//到达区间末尾时，计算最后一秒
            queue1.add(curSecond);
//            calCurSecond(curSecond, Thread.currentThread());
        return cur;
    }

    StringBuilder rest = new StringBuilder();
    private long handleBufByLine(long cur, ByteBuffer byteBuffer, long tail) {
        while (byteBuffer.position() < byteBuffer.capacity()) {
            char ch = (char) byteBuffer.get();
            if (ch == '\n') {
                queue2.add(rest.toString());
                rest = new StringBuilder();
            } else rest.append(ch);
        }
        return cur + byteBuffer.capacity();
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

    //对某一秒的计算任务
    private void calCurSecond(String curSecond, Thread thread) {
//        Map<String, List<Integer>> listMap = threadMap.get(thread);//取出该线程要处理的某一秒
        Map<String, List<Integer>> listMap = timeMap.get(curSecond);//取出该线程要处理的某一秒
        Map<String, String> methodMap = resultMap.get(curSecond);
        if (methodMap == null) {
            methodMap = new HashMap<>();
            resultMap.put(curSecond, methodMap);
        }
        Iterator<String> iterator1 = listMap.keySet().iterator();
        while (iterator1.hasNext()) {
            String methodName = iterator1.next();
            List<Integer> list = listMap.get(methodName);
            Collections.sort(list);
            int qps = list.size();
            int i99 = (int) (list.size() * 0.99);
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
            methodMap.put(methodName.toString(), result);
//            System.out.println(curSecond + " " + methodName + " " + result);
        }
        timeMap.remove(curSecond);
//        listMap.clear();

    }

    /**
     * getResult() 方法是由kcode评测系统调用，是评测程序正确性的一部分，请按照题目要求返回正确数据
     * 输入格式和输出格式参考 README.md
     *
     * @param timestamp  秒级时间戳
     * @param methodName 方法名称
     */
    public String getResult(Long timestamp, String methodName) {
//        if (!resultMap.containsKey(timestamp.toString())) return "";
        String result = resultMap.get(timestamp.toString()).get(methodName);
        return result;
    }
}