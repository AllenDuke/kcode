package com.kuaishou.kcode;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author kcode
 * Created on 2020-06-01
 * 实际提交时请维持包名和类名不变
 */

public class KcodeRpcMonitorImpl implements KcodeRpcMonitor {

    private int threadNum = 1;//16线程会出错

    //数据的特征，似乎是一台机器只布置一个服务，一个服务布置在多台机器
    //但是我们要考虑服务与机器是多对多的关系

    //记录各线程，每分钟的情况

    //       curMinute+callerName+respName
    private final Map<String, List<String>> checkPairMap = new ConcurrentHashMap<>(3840);
    //通过冗余来减少连接       checkPairDataMap
    private final Map<Thread, Map<CheckPairDataKey, InvokeInfo>> threadMap = new HashMap<>();

    //         curMinute+respName
    private final Map<String, CheckResponderInfo> checkResponderDataMap = new ConcurrentHashMap<>(2100);
    //    respName+startTime+endTime
    private final Map<String, String> checkResponderMap = new HashMap<>();

    private final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm");//hh 为12小时制

    private static final List<String> emptyRes = new ArrayList<>();


    // 不要修改访问级别
    public KcodeRpcMonitorImpl() {
    }

    public void prepare(String path) throws Exception {
        FileInputStream inputStream = new FileInputStream(path);
        parseInNIO(inputStream);
//        parseInBIO(inputStream);//200s
    }

    private void parseInNIO(InputStream inputStream) throws IOException, InterruptedException {
        FileInputStream fileInputStream = (FileInputStream) inputStream;
        FileChannel channel = fileInputStream.getChannel();
        long channelSize = channel.size();
        List<Thread> threads = new ArrayList<>(threadNum);

//        CompositeByteBuffer compositeByteBuffer=new CompositeByteBuffer();
//        long start=0;
//        while (start<channelSize){
//            long size=compositeByteBuffer.getSingleCapacity();
//            if(channel.size()-start<size) size=channel.size()-start;
//            //这里只是完成map，在真正get的时候才会去进行io操作
//            compositeByteBuffer.addByteBuffers(channel.map(FileChannel.MapMode.READ_ONLY,start,size));
//            start+=compositeByteBuffer.getSingleCapacity();
//        }

        long begin = 0;
        long len = channelSize / threadNum;//每个线程理论处理大小
        for (int i = 0; i < threadNum; i++) {//分割文件，多个线程并行读
            long end = (begin + len - 1) < channelSize - 1 ? (begin + len - 1) : channelSize - 1;
            long end2 = calRealEndInNIO(end + 1, channel);
            end = end2;
            final long tb = begin;
            final long te = end;
            //当前线程要处理的实际区间是begin~end
            Thread thread = new Thread(() -> {
                Utils.sdsThreadLocal.set(new SDS(6));

                long cur = tb;
                long tail = te;
                try {
//                    CompositeByteBuffer slice = compositeByteBuffer.slice(cur, tail);
//                    handleBuf(cur,slice,tail);

                    while (tail - cur + 1 >= 1024 * 1024 * 1024) {//在当前处理范围内，每64MB建立一个内存映射
                        //理论上在机器物理内存允许的基础上，应该尽可能接近Integer.MAX_VALUE
                        //todo 应该1个线程读的，多个线程对这个buffer进行处理，多个线程读并没有效果，但是我直接分成每个线程一次差不多1G大小也没有效果
                        MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, cur,
                                1024* 1024 * 1024);
                        cur = handleBuf(cur, mappedByteBuffer, tail);//更新cur
                    }
                    if (cur <= tail) {
                        MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, cur,
                                tail - cur + 1);
                        cur = handleBuf(cur, mappedByteBuffer, tail);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            threads.add(thread);
            thread.setName("io thread-" + i);
            threadMap.put(thread, new HashMap<>(3821));
            thread.start();
            begin = end + 1;
        }

        for (Thread thread : threads) {
            thread.join();
        }

        //清算每一分钟resp被调成功率
        for (String s : checkResponderDataMap.keySet()) {
            CheckResponderInfo checkResponderInfo = checkResponderDataMap.get(s);
            checkResponderInfo.setSuccessRate(checkResponderInfo.getSuccessCount() / (checkResponderInfo.getCount() * 1.0));
        }

        Utils.sdsThreadLocal.set(new SDS(6));
    }

    private long handleBuf(long cur, ByteBuffer byteBuffer, long tail) {
        long curMinute = 0;
        SDS callerName = new SDS();
        SDS respName = new SDS();
        SDS callerIP = new SDS();
        SDS respIP=new SDS();
        long begin = 0;
        boolean invokeResult;
        int invokeCost;
        int invokeTime;
        char ch;
        int ci;
        while (true) {//优化了判断条件
            try {//开始处理一行数据
//                long start=System.nanoTime();

                begin = cur + byteBuffer.position();//标记行开始时，channel position所在

                invokeCost = 0;
                invokeTime = 0;

                ch = (char) byteBuffer.get();
                while (ch != ',') {
                    callerName.append(ch);
                    ch = (char) byteBuffer.get();
                }

                ch = (char) byteBuffer.get();
                while (ch != ',') {
                    callerIP.append(ch);
                    ch = (char) byteBuffer.get();
                }

                ch = (char) byteBuffer.get();
                while (ch != ',') {
                    respName.append(ch);
                    ch = (char) byteBuffer.get();
                }


                ch = (char) byteBuffer.get();
                while (ch != ',') {
                    respIP.append(ch);
                    ch = (char) byteBuffer.get();
                }

                ch = (char) byteBuffer.get();
                if (ch == 't') {
                    invokeResult = true;
                    //跳过剩余和 ','
                    for (int i = 0; i < 4; i++) byteBuffer.get();
                } else {
                    invokeResult = false;
                    //跳过剩余和 ','
                    for (int i = 0; i < 5; i++) byteBuffer.get();
                }

                ci = byteBuffer.get();
                while (ci != ',') {
                    invokeCost = invokeCost * 10 + (ci - 48);
                    ci = byteBuffer.get();
                }

                //以分钟为单位
                for (int i = 0; i < 9; i++) {
                    invokeTime = invokeTime * 10 + (byteBuffer.get() - 48);
                }
                //跳过秒最后一位 毫秒和 /n
                for (int i = 0; i < 5; i++) {
                    byteBuffer.get();
                }
                invokeTime /= 6;

                CheckPairDataKey checkPairDataKey = new CheckPairDataKey();
                checkPairDataKey.setCallerName(callerName.toString());
                checkPairDataKey.setRespName(respName.toString());
                checkPairDataKey.setCallerIP(callerIP.toString());
                checkPairDataKey.setRespIP(respIP.toString());

                Thread curThread = Thread.currentThread();

                if (curMinute == 0) {
                    curMinute = invokeTime;
                } else if (invokeTime > curMinute) {//如果已经完整解析一分钟
                    calCurMinute(curMinute * 60 * 1000, threadMap.get(curThread));//结算上一分钟
                    curMinute = invokeTime;
                }

                Map<CheckPairDataKey, InvokeInfo> checkPairDataMap = threadMap.get(curThread);

                InvokeInfo invokeInfo = checkPairDataMap.get(checkPairDataKey);
                if (invokeInfo == null) {
                    invokeInfo = new InvokeInfo();
                    checkPairDataMap.put(checkPairDataKey, invokeInfo);
                }
                invokeInfo.setCount(invokeInfo.getCount() + 1);
                if (invokeResult) invokeInfo.setSuccessCount(invokeInfo.getSuccessCount() + 1);
                invokeInfo.getCosts().add(invokeCost);

//                String s=callerName.toString()+","+respName.toString()+","+callerRespIP.toString()+",
//                "+invokeResult+","
//                        +invokeCost+","+invokeTime;
//                System.out.println(s);

                callerName.clear();
                respName.clear();
                callerIP.clear();
                respIP.clear();

//                System.out.println(System.nanoTime() - start);
            } catch (Exception e) {
//                e.printStackTrace();
//                System.out.println("该行不能完整解析，回退到" + begin);
                cur += byteBuffer.capacity();
                if (cur > tail)//到达区间末尾时，计算最后一秒
                    calCurMinute(curMinute * 60 * 1000, threadMap.get(Thread.currentThread()));
                return begin;
            }
        }
    }

    //寻找出各个线程要处理的边界
    private long calRealEndInNIO(long cur, FileChannel channel) throws IOException {
        long deadMills = 0;
        long len = 300 * 1024 * 1024;
        long size = channel.size() - cur > len ? len : channel.size() - cur;
        MappedByteBuffer byteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, cur, size);
        //从最近的换行符开始计算
        while (byteBuffer.position() < byteBuffer.capacity() && byteBuffer.get() != '\n') ;
        while (byteBuffer.position() < byteBuffer.capacity()) {//尝试在接下来的250MB内存中找
            long begin = cur + byteBuffer.position();//标记行开始
            //这里偷懒了，偷懒的前提是保证在250MB内找到
            int count = 0;
            while (count < 6) {
                if (byteBuffer.get() == ',') count++;
            }

            /**
             * 取invokeTime，截掉毫秒
             */
            long invokeTime = 0;
            count = 0;
            while (count < 10) {
                int i = byteBuffer.get();
                invokeTime = invokeTime * 10 + (i - 48);
                count++;
            }
            invokeTime *= 1000;
            for (int i = 0; i < 3; i++) byteBuffer.get();

            if (deadMills == 0) {
                Date date = new Date(invokeTime);
                date.setSeconds(0);

                date.setMinutes(date.getMinutes() + 1);
                deadMills = date.getTime();
            } else if (invokeTime >= deadMills) {//寻找到完整的一分钟
//                System.out.println(new Date(curMills) + " " + new Date(deadMills));
                return begin - 1;//注意
            }
            while (byteBuffer.get() != '\n') ;
        }
        return cur - 1;
    }

    //计算每一分钟的任务
    private void calCurMinute(long curMills, Map<CheckPairDataKey, InvokeInfo> checkPairDataMap) {
        long start = System.currentTimeMillis();
        String curDate = format.format(new Date(curMills));
        String callerName;
        String respName;
        String callerRespIP;
        InvokeInfo invokeInfo;
        List<Integer> costs;
        int i99;
        int P99;
        double successRate;
        String checkKey;
        List<String> checkResult;
        String responderKey;
        for (CheckPairDataKey checkPairDataKey : checkPairDataMap.keySet()) {
            callerName = checkPairDataKey.getCallerName();
            respName = checkPairDataKey.getRespName();
            callerRespIP = checkPairDataKey.getCallerIP()+","+checkPairDataKey.getRespIP();
            invokeInfo = checkPairDataMap.get(checkPairDataKey);
            costs = invokeInfo.getCosts();
            Collections.sort(costs);
            i99 = (int) (costs.size() * 0.99);//使用类库会有不一样的效果吗
            if (costs.size() % 100 != 0) i99++;
            P99 = costs.get(i99 - 1);
//            int P99 = costs.get((int) Math.ceil(costs.size() * 0.99) - 1);
            successRate = invokeInfo.getSuccessCount() / (invokeInfo.getCount() * 1.0);

            checkKey = curDate + callerName + respName;
            checkResult = checkPairMap.get(checkKey);
            if (checkResult == null) {
                checkResult = new ArrayList<>();
                checkPairMap.put(checkKey, checkResult);
            }
            checkResult.add(callerRespIP + "," + Utils.doubleToPersentage(successRate) + "," + P99);

            responderKey = curDate + respName;
            CheckResponderInfo checkResponderInfo = checkResponderDataMap.get(responderKey);
            if (checkResponderInfo == null) {
                checkResponderInfo = new CheckResponderInfo();
                checkResponderDataMap.put(responderKey, checkResponderInfo);
            }
            checkResponderInfo.setSuccessCount(checkResponderInfo.getSuccessCount() + invokeInfo.getSuccessCount());
            checkResponderInfo.setCount(checkResponderInfo.getCount() + invokeInfo.getCount());
        }

        checkPairDataMap.clear();

        System.out.println(curDate + " " + (System.currentTimeMillis() - start));
    }

    public List<String> checkPair(String caller, String responder, String time) {
        List<String> result = checkPairMap.get(time + caller + responder);
        if (result == null) return emptyRes;
        return result;
    }

    public String checkResponder(String responder, String start, String end) {
        String key = responder + start + end;
        String answer = checkResponderMap.get(key);//有大量的重复询问
        if (answer != null) return answer;
        double result = 0;
        int sum = 0;
        CheckResponderInfo checkResponderInfo;
        while (start.compareTo(end) <= 0) {//聚合成一个key
            checkResponderInfo = checkResponderDataMap.get(start + responder);
            start = Utils.dateIncreaseMinute(start);
            if (checkResponderInfo == null) continue;//这一分钟responder没有被调用
            result += checkResponderInfo.getSuccessRate();
            sum++;
        }
        if (sum == 0) answer = "-1.00%";
        else answer = Utils.doubleToPersentage(result / sum);
        checkResponderMap.put(key, answer);
        return answer;
    }

}
