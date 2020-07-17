package com.kuaishou.kcode;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author KCODE
 * Created on 2020-07-04
 */
public class KcodeAlertAnalysisImpl implements KcodeAlertAnalysis {

    private int ioThreadNum =  1;//似乎多线程没有效果，或者效果不大

    private int beginTime = Integer.MAX_VALUE;

    private int endTime = 0;

    private Map<Integer, Map<NameIPPairDataKey, InvokeInfo>> timeMap = new ConcurrentHashMap<>();

    //主被调ip对按分钟聚合结后的结果
    private final Map<Integer, List<NameIPPairData>> nameIPPairGatherResultMap = new ConcurrentHashMap<>();

    //nameIPPairGatherResultMap再按namePair聚合
    private Map<String, NamePariDataValue> namePairGatherResultMap = new ConcurrentHashMap<>(3000);

    private Map<String, Set<String>> callerRespsMap = new ConcurrentHashMap<>(40);
    private Map<String, Set<String>> respCallersMap = new ConcurrentHashMap<>(40);

    //实际报警信息
    private Collection<String> alertInfos;

    //节省20s
    private ThreadPoolExecutor executor = new ThreadPoolExecutor(2, 2, 30, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>());

    //caller.hash<<32|resp.hash time<<x
    private final Map<Long, Map<Integer, List<String>>> longestPathResultMap = new HashMap<>(250);

    //这是线程不安全的 巨坑
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");//hh 为12小时制

    //用于将百分数转换为double 这是线程不安全的 巨坑
    private static final NumberFormat numberFormat = NumberFormat.getPercentInstance();

    private void parseInNIO(InputStream inputStream) throws IOException, InterruptedException {
        FileInputStream fileInputStream = (FileInputStream) inputStream;
        FileChannel channel = fileInputStream.getChannel();
        long channelSize = channel.size();
        List<Thread> threads = new ArrayList<>(ioThreadNum);

        long begin = 0;
        long len = channelSize / ioThreadNum;//每个线程理论处理大小
        for (int i = 0; i < ioThreadNum; i++) {//分割文件，多个线程并行读
            long end = (begin + len - 1) < channelSize - 1 ? (begin + len - 1) : channelSize - 1;
            end = calRealBeginAndEndInNIO(end + 1, channel);

            final long tb = begin;
            final long te = end;
            //当前线程要处理的实际区间是begin~end
            Thread thread = new Thread(() -> {

                long cur = tb;
                long tail = te;
                try {
                    while (tail - cur + 1 >= 1024 * 1024 * 1024) {//在当前处理范围内，每64MB建立一个内存映射
                        //理论上在机器物理内存允许的基础上，应该尽可能接近Integer.MAX_VALUE
                        MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, cur,
                                1024 * 1024 * 1024);
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
            thread.start();
            begin = end + 1;
        }

        for (Thread thread : threads) {
            thread.join();
        }
    }

    private long handleBuf(long cur, ByteBuffer byteBuffer, long tail) {
        SDS callerName = new SDS();
        SDS respName = new SDS();
        SDS callerIP = new SDS();
        SDS respIP = new SDS();
        long begin = 0;
        boolean invokeResult;
        int invokeCost;
        int invokeTime;
        char ch;
        int ci;
        NameIPPairDataKey nameIPPairDataKey;
        Map<NameIPPairDataKey, InvokeInfo> dataMap = null;//当前分钟的数据map
        InvokeInfo invokeInfo = null;
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

                Map<NameIPPairDataKey, InvokeInfo> readyDataMap = timeMap.get(invokeTime - 2);
                if (readyDataMap != null) {//确定当前的2分钟前的数据已经准备好，可以结算
                    final int readyTime = invokeTime - 2;
                    final Map<NameIPPairDataKey, InvokeInfo> finalReadyMap = readyDataMap;
                    timeMap.remove(readyTime);
                    executor.execute(() ->calCurMinute(readyTime, finalReadyMap));
                }

                nameIPPairDataKey = new NameIPPairDataKey();
                nameIPPairDataKey.setCallerName(callerName.toString());
                nameIPPairDataKey.setRespName(respName.toString());
                nameIPPairDataKey.setCallerIP(callerIP.toString());
                nameIPPairDataKey.setRespIP(respIP.toString());

                dataMap = timeMap.get(invokeTime);
                if (dataMap == null) {
                    dataMap = new ConcurrentHashMap<>(4200);
                    timeMap.put(invokeTime, dataMap);
                }

                invokeInfo = dataMap.get(nameIPPairDataKey);
                if (invokeInfo == null) {
                    invokeInfo = new InvokeInfo();
                    dataMap.put(nameIPPairDataKey, invokeInfo);
                }
                invokeInfo.setCount(invokeInfo.getCount() + 1);
                if (invokeResult) invokeInfo.setSuccessCount(invokeInfo.getSuccessCount() + 1);
                invokeInfo.getCosts().add(invokeCost);

                callerName.clear();
                respName.clear();
                callerIP.clear();
                respIP.clear();

//                System.out.println(System.nanoTime() - start);
            } catch (Exception e) {
//                e.printStackTrace();
//                System.out.println("该行不能完整解析，回退到" + begin);
                cur += byteBuffer.capacity();
                return begin;
            }
        }
    }

    //寻找出各个线程要处理的区间
    private long calRealBeginAndEndInNIO(long cur, FileChannel channel) throws IOException {
        if (cur == channel.size()) return cur - 1;
        long len = 10 * 1024 * 1024;
        long size = channel.size() - cur > len ? len : channel.size() - cur;
        MappedByteBuffer byteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, cur, size);
        //从最近的换行符开始计算
        while (byteBuffer.get() != '\n') ;
        return cur + byteBuffer.position() - 1;
    }

    //清理剩余的timeMap
    private void clearTimeMap() {
        for (Integer time : timeMap.keySet()) {//最后两秒，必定一奇一偶
            if ((time & 1) == 0) {//由线程池处理
                executor.execute(() -> {
                    calCurMinute(time, timeMap.get(time));
                    timeMap.remove(time);
                });
            } else {//主线程处理
                calCurMinute(time, timeMap.get(time));
                timeMap.remove(time);
            }
        }
        while (timeMap.size() > 0) ;//自旋等待timeMap被清算完
    }

    //计算每一分钟的任务 同时完善调用链路关系
    private void calCurMinute(int curMinute, Map<NameIPPairDataKey, InvokeInfo> pairDataMap) {
//        long start = System.currentTimeMillis();

        //用于聚合服务
        Map<NamePairDataKey, InvokeInfo> namePairInvokeInfoMap = new HashMap<>();

        List<NameIPPairData> nameIPPairDataList = new ArrayList<>(4200);

        String curDate = ConcurrentDateFormat.format(new Date((long) curMinute * 60000));
        InvokeInfo invokeInfo;
        List<Integer> costs;
        int i99;
        int P99;
        double successRate;
        for (NameIPPairDataKey nameIPPairDataKey : pairDataMap.keySet()) {
            invokeInfo = pairDataMap.get(nameIPPairDataKey);
            costs = invokeInfo.getCosts();
            Collections.sort(costs);
            i99 = (int) (costs.size() * 0.99);
            if (costs.size() % 100 != 0) i99++;
            P99 = costs.get(i99 - 1);
            successRate = (double) invokeInfo.getSuccessCount() / invokeInfo.getCount();
            NameIPPairData nameIPPairData = new NameIPPairData();
            nameIPPairData.setCallerName(nameIPPairDataKey.getCallerName());
            nameIPPairData.setRespName(nameIPPairDataKey.getRespName());
            nameIPPairData.setCallerIP(nameIPPairDataKey.getCallerIP());
            nameIPPairData.setRespIP(nameIPPairDataKey.getRespIP());
            nameIPPairData.setP99(P99);
            nameIPPairData.setSR(successRate);
            nameIPPairDataList.add(nameIPPairData);

            NamePairDataKey namePairDataKey = new NamePairDataKey();
            namePairDataKey.setCallerName(nameIPPairDataKey.getCallerName());
            namePairDataKey.setRespName(nameIPPairDataKey.getRespName());
            InvokeInfo info = namePairInvokeInfoMap.get(namePairDataKey);
            if (info == null) {
                info = new InvokeInfo();
                namePairInvokeInfoMap.put(namePairDataKey, info);
            }
            info.getCosts().addAll(invokeInfo.getCosts());
            info.setCount(info.getCount() + invokeInfo.getCount());
            info.setSuccessCount(info.getSuccessCount() + invokeInfo.getSuccessCount());
        }
        //todo CopyOnWrite
//        nameIPPairGatherResultMap.put(curMinute, new CopyOnWriteArrayList<>(nameIPPairDataList));
        nameIPPairGatherResultMap.put(curMinute, nameIPPairDataList);

        for (NamePairDataKey namePairDataKey : namePairInvokeInfoMap.keySet()) {
            invokeInfo = namePairInvokeInfoMap.get(namePairDataKey);
            costs = invokeInfo.getCosts();
            Collections.sort(costs);
            i99 = (int) (costs.size() * 0.99);
            if (costs.size() % 100 != 0) i99++;
            P99 = costs.get(i99 - 1);
            successRate = (double) invokeInfo.getSuccessCount() / invokeInfo.getCount();
            namePairGatherResultMap.put(namePairDataKey.getCallerName() + namePairDataKey.getRespName() + curDate,
                    new NamePariDataValue(P99, successRate));

            Set<String> resps = callerRespsMap.get(namePairDataKey.getCallerName());
            if (resps == null) {
                resps = new HashSet<>();
                callerRespsMap.put(namePairDataKey.getCallerName(), resps);
            }
            resps.add(namePairDataKey.getRespName());

            Set<String> callers = respCallersMap.get(namePairDataKey.getRespName());
            if (callers == null) {
                callers = new HashSet<>();
                respCallersMap.put(namePairDataKey.getRespName(), callers);
            }
            callers.add(namePairDataKey.getCallerName());
        }

//        System.out.println(curDate + " " + (System.currentTimeMillis() - start));
    }

    //计算当前规则 并不耗时
    public Collection<String> calAlertRules(Collection<String> alertRules) throws Exception {
        Collection<String> re = new LinkedList<>();
        SDS sds = new SDS(200);
        String[] split;
        String ruleId;
        String callerName;
        String respName;
        String dataType;
        int count;//连续次数
        boolean large;// > <，一般 > 与P99绑定， < 与SR绑定
        String condition;
        String threshold;
        double thresholdSR;
        int thresholdP99;
        for (String alertRule : alertRules) {//遍历规则
            split = alertRule.split(",");
            ruleId = split[0];
            callerName = split[1];
            respName = split[2];
            dataType = split[3];
            condition = split[4];
            count = Integer.valueOf(condition.substring(0, condition.length() - 1));
            if (condition.charAt(condition.length() - 1) == '>') large = true;
            threshold = split[5];
            //优化 提前判断
            if (dataType.equals("SR")) {//一般而言，SR对应 <
//                thresholdSR = numberFormat.parse(threshold).doubleValue();
                thresholdSR=ConcurrentNumberFormat.toDouble(threshold);

                if (callerName.equals("ALL")) {
                    Set<String> passedCallerRespNameIPTimeSet = new HashSet<>();//callerName respName不确定

                    for (int time = beginTime; time <= endTime; time++) {//遍历时间
                        List<NameIPPairData> nameIPPairDataList = nameIPPairGatherResultMap.get(time);//假定连续
                        for (NameIPPairData nameIPPairData : nameIPPairDataList) {//遍历该时间下的pairData
                            if (nameIPPairData.getRespName().equals(respName)) {
                                if (nameIPPairData.getSR() < thresholdSR) {
                                    passedCallerRespNameIPTimeSet.add(nameIPPairData.getCallerName() + nameIPPairData.getRespName()
                                            + nameIPPairData.getCallerIP() + nameIPPairData.getRespIP() + time);
                                    boolean ling = true;
                                    for (int i = time - 1; i >= (time - count + 1); i--)
                                        if (!passedCallerRespNameIPTimeSet.contains(nameIPPairData.getCallerName() + nameIPPairData.getRespName()
                                                + nameIPPairData.getCallerIP() + nameIPPairData.getRespIP() + i)) {//不连续
                                            ling = false;
                                            break;
                                        }
                                    if (ling) {//报警
                                        sds.clear();
                                        sds.append(ruleId).append(",")
                                                .append(ConcurrentDateFormat.format(new Date((long) time * 60000))).append(",")
                                                .append(nameIPPairData.getCallerName()).append(",")
                                                .append(nameIPPairData.getCallerIP()).append(",")
                                                .append(nameIPPairData.getRespName()).append(",")
                                                .append(nameIPPairData.getRespIP()).append(",")
                                                .append(Utils.doubleToPersentage(nameIPPairData.getSR()));
                                        String result = sds.toString();
                                        re.add(result);
                                    }
                                }
                            }
                        }
                    }
                } else if (respName.equals("ALL")) {//下游所有
                    //找出包含自己在内的所有下游
//                    Set<String> downStreamSet = new HashSet<>();
//                    downStreamSet.add(callerName);

                    Set<String> passedCallerRespNameIPTimeSet = new HashSet<>();//callerName respName不确定

                    for (int time = beginTime; time <= endTime; time++) {//遍历时间
                        List<NameIPPairData> nameIPPairDataList = nameIPPairGatherResultMap.get(time);//假定连续
                        for (NameIPPairData nameIPPairData : nameIPPairDataList) {//遍历该时间下的键值对
//                            if (downStreamSet.contains(nameIPPairData.getCallerName())) {
//                                downStreamSet.add(nameIPPairData.getRespName());//当前被调成为下游
                            if (nameIPPairData.getCallerName().equals(callerName)) {
                                if (nameIPPairData.getSR() < thresholdSR) {
                                    passedCallerRespNameIPTimeSet.add(nameIPPairData.getCallerName() + nameIPPairData.getRespName()
                                            + nameIPPairData.getCallerIP() + nameIPPairData.getRespIP() + time);
                                    boolean ling = true;
                                    for (int i = time - 1; i >= (time - count + 1); i--)
                                        if (!passedCallerRespNameIPTimeSet.contains(nameIPPairData.getCallerName() + nameIPPairData.getRespName()
                                                + nameIPPairData.getCallerIP() + nameIPPairData.getRespIP() + i)) {//不连续
                                            ling = false;
                                            break;
                                        }
                                    if (ling) {//报警
                                        sds.clear();
                                        sds.append(ruleId).append(",")
                                                .append(ConcurrentDateFormat.format(new Date((long) time * 60000))).append(",")
                                                .append(nameIPPairData.getCallerName()).append(",")
                                                .append(nameIPPairData.getCallerIP()).append(",")
                                                .append(nameIPPairData.getRespName()).append(",")
                                                .append(nameIPPairData.getRespIP()).append(",")
                                                .append(Utils.doubleToPersentage(nameIPPairData.getSR()));
                                        String result = sds.toString();
                                        re.add(result);
                                    }
                                }
                            }
                        }
                    }
                } else {
//                    Set<Integer> passedTimeSet = new HashSet<>();//已确定callerName respName
                    Set<String> passedCallerRespIPTime = new HashSet<>();

                    for (int time = beginTime; time <= endTime; time++) {//遍历时间
                        List<NameIPPairData> nameIPPairDataList = nameIPPairGatherResultMap.get(time);//假定连续
                        for (NameIPPairData nameIPPairData : nameIPPairDataList) {//遍历该时间下的键值对
                            if (nameIPPairData.getCallerName().equals(callerName) && nameIPPairData.getRespName().equals(respName)) {
                                if (nameIPPairData.getSR() < thresholdSR) {
                                    passedCallerRespIPTime.add(nameIPPairData.getCallerIP() + nameIPPairData.getRespIP() + time);
                                    boolean ling = true;
                                    for (int i = time - 1; i >= (time - count + 1); i--)
                                        if (!passedCallerRespIPTime.contains(nameIPPairData.getCallerIP() + nameIPPairData.getRespIP() + i)) {//不连续
                                            ling = false;
                                            break;
                                        }
                                    if (ling) {//报警
                                        sds.clear();
                                        sds.append(ruleId).append(",")
                                                .append(ConcurrentDateFormat.format(new Date((long) time * 60000))).append(",")
                                                .append(nameIPPairData.getCallerName()).append(",")
                                                .append(nameIPPairData.getCallerIP()).append(",")
                                                .append(nameIPPairData.getRespName()).append(",")
                                                .append(nameIPPairData.getRespIP()).append(",")
                                                .append(Utils.doubleToPersentage(nameIPPairData.getSR()));
                                        String result = sds.toString();
                                        re.add(result);
                                    }
                                }
                            }
                        }
                    }
                }
            } else {//dataType.equals("P99") //一般而言，P99对应 >
                thresholdP99 = Integer.valueOf(threshold.substring(0, threshold.length() - 2));

                if (callerName.equals("ALL")) {
                    Set<String> passedCallerRespNameIPTimeSet = new HashSet<>();//callerName respName不确定

                    for (int time = beginTime; time <= endTime; time++) {//遍历时间
                        List<NameIPPairData> nameIPPairDataList = nameIPPairGatherResultMap.get(time);//假定连续
                        for (NameIPPairData nameIPPairData : nameIPPairDataList) {//遍历该时间下的键值对
                            if (nameIPPairData.getRespName().equals(respName)) {
                                if (nameIPPairData.getP99() > thresholdP99) {
                                    passedCallerRespNameIPTimeSet.add(nameIPPairData.getCallerName() + nameIPPairData.getRespName()
                                            + nameIPPairData.getCallerIP() + nameIPPairData.getRespIP() + time);
                                    boolean ling = true;
                                    for (int i = time - 1; i >= (time - count + 1); i--)
                                        if (!passedCallerRespNameIPTimeSet.contains(nameIPPairData.getCallerName() + nameIPPairData.getRespName()
                                                + nameIPPairData.getCallerIP() + nameIPPairData.getRespIP() + i)) {//不连续
                                            ling = false;
                                            break;
                                        }
                                    if (ling) {//报警
                                        sds.clear();
                                        sds.append(ruleId).append(",")
                                                .append(ConcurrentDateFormat.format(new Date((long) time * 60000))).append(",")
                                                .append(nameIPPairData.getCallerName()).append(",")
                                                .append(nameIPPairData.getCallerIP()).append(",")
                                                .append(nameIPPairData.getRespName()).append(",")
                                                .append(nameIPPairData.getRespIP()).append(",")
                                                .append(String.valueOf(nameIPPairData.getP99())).append("ms");
                                        String result = sds.toString();
                                        re.add(result);
                                    }
                                }
                            }
                        }
                    }
                } else if (respName.equals("ALL")) {//下游所有
                    //找出包含自己在内的所有下游
//                    Set<String> downStreamSet=new HashSet<>();
//                    downStreamSet.add(callerName);

                    Set<String> passedCallerRespNameIPTimeSet = new HashSet<>();//callerName respName不确定

                    for (int time = beginTime; time <= endTime; time++) {//遍历时间
                        List<NameIPPairData> nameIPPairDataList = nameIPPairGatherResultMap.get(time);
                        for (NameIPPairData nameIPPairData : nameIPPairDataList) {//遍历该时间下的键值对
//                            if (downStreamSet.contains(nameIPPairData.getCallerName())) {
//                                downStreamSet.add(nameIPPairData.getRespName());//当前被调成为下游
                            if (nameIPPairData.getCallerName().equals(callerName)) {
                                if (nameIPPairData.getP99() > thresholdP99) {
                                    passedCallerRespNameIPTimeSet.add(nameIPPairData.getCallerName() + nameIPPairData.getRespName()
                                            + nameIPPairData.getCallerIP() + nameIPPairData.getRespIP() + time);
                                    boolean ling = true;
                                    for (int i = time - 1; i >= (time - count + 1); i--)
                                        if (!passedCallerRespNameIPTimeSet.contains(nameIPPairData.getCallerName() + nameIPPairData.getRespName()
                                                + nameIPPairData.getCallerIP() + nameIPPairData.getRespIP() + i)) {//不连续
                                            ling = false;
                                            break;
                                        }
                                    if (ling) {//报警
                                        sds.clear();
                                        sds.append(ruleId).append(",")
                                                .append(ConcurrentDateFormat.format(new Date((long) time * 60000))).append(",")
                                                .append(nameIPPairData.getCallerName()).append(",")
                                                .append(nameIPPairData.getCallerIP()).append(",")
                                                .append(nameIPPairData.getRespName()).append(",")
                                                .append(nameIPPairData.getRespIP()).append(",")
                                                .append(String.valueOf(nameIPPairData.getP99())).append("ms");
                                        String result = sds.toString();
                                        re.add(result);
                                    }
                                }
                            }
                        }
                    }
                } else {
                    Set<String> passedCallerRespIPTime = new HashSet<>();

                    for (int time = beginTime; time <= endTime; time++) {//遍历时间
                        List<NameIPPairData> nameIPPairDataList = nameIPPairGatherResultMap.get(time);
                        for (NameIPPairData nameIPPairData : nameIPPairDataList) {//遍历该时间下的键值对
                            if (nameIPPairData.getCallerName().equals(callerName) && nameIPPairData.getRespName().equals(respName)) {
                                if (nameIPPairData.getP99() > thresholdP99) {
                                    passedCallerRespIPTime.add(nameIPPairData.getCallerIP() + nameIPPairData.getRespIP() + time);
                                    boolean ling = true;
                                    for (int i = time - 1; i >= (time - count + 1); i--)
                                        if (!passedCallerRespIPTime.contains(nameIPPairData.getCallerIP() + nameIPPairData.getRespIP() + i)) {//不连续
                                            ling = false;
                                            break;
                                        }
                                    if (ling) {//报警
                                        sds.clear();
                                        sds.append(ruleId).append(",")
                                                .append(ConcurrentDateFormat.format(new Date((long) time * 60000))).append(",")
                                                .append(nameIPPairData.getCallerName()).append(",")
                                                .append(nameIPPairData.getCallerIP()).append(",")
                                                .append(nameIPPairData.getRespName()).append(",")
                                                .append(nameIPPairData.getRespIP()).append(",")
                                                .append(String.valueOf(nameIPPairData.getP99())).append("ms");
                                        String result = sds.toString();
                                        re.add(result);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return re;
    }

    //确定开始与结束分钟数
    private void getBeginAndEnd() {
        for (Integer time : nameIPPairGatherResultMap.keySet()) {//避免排序
            if (time < beginTime) beginTime = time;
            if (time > endTime) endTime = time;
        }
    }

    //用主线程去预热getLongestPath的数据
    private void longestPathWarmUp(int beginT, int endT) {
//        long start=System.currentTimeMillis();
        int time;
        for (time = beginT; time <= endT; time++) {
            String curDate = ConcurrentDateFormat.format(new Date((long) time * 60000));
            for (String caller : callerRespsMap.keySet()) {
                for (String resp : callerRespsMap.get(caller)) {
                    getLongestPathResult(caller, resp, curDate, "P99");
                    getLongestPathResult(caller, resp, curDate, "SR");
                }
            }
        }
//        long warmUpFinsh=System.currentTimeMillis()-start;
//        System.out.println("warmUp cost：" + warmUpFinsh + "ms");
    }

    @Override
    public Collection<String> alarmMonitor(String path, Collection<String> alertRules) throws Exception {
        FileInputStream inputStream = new FileInputStream(path);

//        long start=System.currentTimeMillis();
        parseInNIO(inputStream);//比BIO快约10s

        clearTimeMap();

        getBeginAndEnd();

        executor.execute(()->longestPathWarmUp(beginTime, endTime));

        executor.shutdown();

//        long parseFinsh=System.currentTimeMillis()-start;


//        start=System.currentTimeMillis();

        this.alertInfos = calAlertRules(alertRules);

//        long alertFinish=System.currentTimeMillis()-start;

        //多个线程去预热数据
//        start=System.currentTimeMillis();
//        longestPathWarmUp(beginTime, endTime);
//        long warmUpFinsh=System.currentTimeMillis()-start;

//        while(this.alertInfos==null);//确保规则以计算完毕
        while (!executor.isTerminated());
        return this.alertInfos;
    }

    private Map<String, List<List<String>>> longestUpPathMap = new HashMap<>(40);
    List<List<String>> longestUpPaths;

    //往上查找最长路径
    private void traceUp(String resp, LinkedList<String> path) {
        path.addFirst(resp);
        if (!respCallersMap.containsKey(resp)) {
            LinkedList<String> curPath = new LinkedList<>();
            curPath.addAll(path);
            if (longestUpPaths.size() == 0) longestUpPaths.add(curPath);
            else {
                if (path.size() == longestUpPaths.get(0).size()) longestUpPaths.add(curPath);
                if (path.size() > longestUpPaths.get(0).size()) {
                    longestUpPaths.clear();
                    longestUpPaths.add(curPath);
                }
            }
            return;
        }
        for (String caller : respCallersMap.get(resp)) {
            traceUp(caller, path);
            path.removeFirst();
        }
    }

    private Map<String, List<List<String>>> longestDownPathMap = new HashMap<>(40);
    List<List<String>> longestDownPaths;

    //往下寻找最长路径
    private void traceDown(String caller, LinkedList<String> path) {
        path.add(caller);
        if (!callerRespsMap.containsKey(caller)) {
            LinkedList<String> curPath = new LinkedList<>();
            curPath.addAll(path);
            if (longestDownPaths.size() == 0) longestDownPaths.add(curPath);
            else {
                if (path.size() == longestDownPaths.get(0).size()) longestDownPaths.add(curPath);
                if (path.size() > longestDownPaths.get(0).size()) {
                    longestDownPaths.clear();
                    longestDownPaths.add(curPath);
                }
            }
            return;
        }
        for (String resp : callerRespsMap.get(caller)) {
            traceDown(resp, path);
            path.removeLast();
        }
    }

    private Map<String, List<List<String>>> longestPathMap = new HashMap<>(250);

    private SDS pathSDS = new SDS(2000);
    private SDS nodeResult = new SDS(800);

    private void getLongestPathResult(String caller, String responder, String time, String type) {
//        long longKey = (nameHash(caller) << 32) | nameHash(responder);
        long longKey=(caller.hashCode()<<32)|responder.hashCode();
        int timeKey=time.hashCode();
//        int timeKey = timeHash(time);
        if (type.charAt(0) == 'P') timeKey = timeKey << 1;

        Map<Integer, List<String>> timePathResultMap = longestPathResultMap.get(longKey);
        if (timePathResultMap == null) {
            timePathResultMap = new HashMap<>();
            longestPathResultMap.put(longKey, timePathResultMap);
        }

        List<String> pathResults = new ArrayList<>(300);
        timePathResultMap.put(timeKey, pathResults);

        //找出callerResp所在的最长链路集合
        String callerResp = caller + responder;
        List<List<String>> paths = longestPathMap.get(callerResp);
        if (paths == null) {
            paths = new ArrayList<>(300);
            longestPathMap.put(callerResp, paths);

            //此节点往上最长路径集合
            longestUpPaths = longestUpPathMap.get(caller);
            if (longestUpPaths == null) {
                longestUpPaths = new ArrayList<>(150);
                traceUp(caller, new LinkedList<>());
                longestUpPathMap.put(caller, longestUpPaths);
            }

            //此节点往下最长路径集合
            longestDownPaths = longestDownPathMap.get(responder);
            if (longestDownPaths == null) {
                longestDownPaths = new ArrayList<>(150);
                traceDown(responder, new LinkedList<>());
                longestDownPathMap.put(responder, longestDownPaths);
            }

            //组装上下集合
            for (List<String> longestUpPath : longestUpPaths) {
                for (List<String> longestDownPath : longestDownPaths) {
                    List<String> path = new ArrayList<>(300);
                    path.addAll(longestUpPath);
                    path.addAll(longestDownPath);
                    paths.add(path);
                }
            }
        }
        if (type.equals("SR")) {
            for (List<String> path : paths) {
                pathSDS.clear();
                nodeResult.clear();
                String pre = path.get(0);
                pathSDS.append(pre).append("->");
                for (int i = 1; i < path.size(); i++) {
                    NamePariDataValue namePariDataValue = namePairGatherResultMap.get(pre + path.get(i) + time);
                    if (i != path.size() - 1) {
                        pathSDS.append(path.get(i)).append("->");
                        if (namePariDataValue == null) nodeResult.append("-1%,");
                        else {
                            double sr = namePariDataValue.getSR();
                            nodeResult.append(Utils.doubleToPersentage(sr)).append(',');
                        }
                    } else {
                        pathSDS.append(path.get(i)).append('|');
                        if (namePariDataValue == null) nodeResult.append("-1%");
                        else {
                            double sr = namePariDataValue.getSR();
                            nodeResult.append(Utils.doubleToPersentage(sr));
                        }
                    }
                    pre = path.get(i);
                }
                pathResults.add(pathSDS.append(nodeResult).toString());
            }
        } else {
            for (List<String> path : paths) {
                pathSDS.clear();
                nodeResult.clear();
                String pre = path.get(0);
                pathSDS.append(pre).append("->");
                for (int i = 1; i < path.size(); i++) {
                    NamePariDataValue namePariDataValue = namePairGatherResultMap.get(pre + path.get(i) + time);
                    if (i != path.size() - 1) {
                        pathSDS.append(path.get(i)).append("->");
                        if (namePariDataValue == null) nodeResult.append("-1ms,");
                        else nodeResult.append(String.valueOf(namePariDataValue.getP99())).append("ms,");
                    } else {
                        pathSDS.append(path.get(i)).append('|');
                        if (namePariDataValue == null) nodeResult.append("-1ms");
                        else nodeResult.append(String.valueOf(namePariDataValue.getP99())).append("ms");
                    }
                    pre = path.get(i);
                }
                pathResults.add(pathSDS.append(nodeResult).toString());
            }
        }
    }

    private int nameHash(String name){
        int hash=name.charAt(3);
        hash<<=8;
        hash|=name.charAt(name.length()/2);
        hash<<=8;
        hash|=name.charAt(name.length()-5);
        hash<<=8;
        hash|=name.length();
        return hash;
    }

    //两个字符足以确定 todo 为什么比hashCode()要慢？
    private int timeHash(String time){
        int hash=time.charAt(time.length()-2)<<8;
        hash|=time.charAt(time.length()-1);
        return hash;
    }

    @Override
    public Collection<String> getLongestPath(String caller, String responder, String time, String type) {
        //String作key要经历的hashCode equals都是耗时操作

//        long longKey = (nameHash(caller) << 32) | nameHash(responder);
        long longKey=(caller.hashCode()<<32)|responder.hashCode();
        int timeKey=time.hashCode();
//        int timeKey = timeHash(time);
        if (type.charAt(0) == 'P') timeKey = timeKey << 1;
        List<String> pathResults = longestPathResultMap.get(longKey).get(timeKey);

        return pathResults;
    }
}