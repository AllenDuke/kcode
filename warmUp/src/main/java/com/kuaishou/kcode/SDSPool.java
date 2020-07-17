package com.kuaishou.kcode;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author 杜科
 * @description SDS 缓存池，线程安全
 * @contact AllenDuke@163.com
 * @date 2020/6/9
 */
public class SDSPool {

    private static Queue<SDS> queue = new ConcurrentLinkedQueue<>();
//
//    private static Queue<SDS> outside=new ConcurrentLinkedQueue<>();
//
//    private static volatile boolean stopClean;

//    private static Thread cleaner=new Thread(()->{
//        while (!stopClean){
//            SDS sds = outside.poll();
//            if(sds!=null&&!sds.isUsing()) {
//                sds.clear();
//                queue.add(sds);
//            }
//        }
//    });

    static {
        for (int i = 0; i < 1000; i++) {
            queue.add(new SDS());
        }
//        cleaner.start();
    }

    public static SDS get() {
        SDS sds = queue.poll();
        if (sds == null) {
            synchronized (queue) {
                sds = queue.poll();
                if (sds == null){
                    for (int i = 0; i < 500; i++) {
                        queue.add(new SDS());
                    }
                }
            }
            return get();
        }
//        sds.setUsing(true);
//        outside.add(sds);
        return sds;
    }

    public static void release(SDS sds) {
//        sds.setUsing(false);
        sds.clear();
        queue.add(sds);
    }
//
//    public static void stopClean(){
//        stopClean=true;
//    }
}
