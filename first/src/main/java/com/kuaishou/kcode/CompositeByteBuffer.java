package com.kuaishou.kcode;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author 杜科
 * @description 组合ByteBuffer，为多个ByteBuffer提供一个组合视图 非线程安全
 * @contact AllenDuke@163.com
 * @date 2020/6/28
 */
public class CompositeByteBuffer{

    //除了第一个和最后一个，其他均一定为singleCapacity
    private List<ByteBuffer> byteBuffers=new ArrayList<>(16);

    private long capacity=0;

    private long singleCapacity=1024*1024*1024;

    private int index=0;

    //从当前CompositeByteBuffer当中再切割出一个新的CompositeByteBuffer
    public CompositeByteBuffer slice(long begin,long tail) {
        CompositeByteBuffer compositeByteBuffer = new CompositeByteBuffer();
        int i= (int) (begin/singleCapacity);
        int j= (int) (tail/singleCapacity);
        while(i<=j){
            ByteBuffer byteBuffer = byteBuffers.get(i);
            int offset= (int) (begin-singleCapacity*i);
            byteBuffer.position(offset);
            int lim=byteBuffer.capacity();
            if(tail-singleCapacity*i+1<byteBuffer.capacity()) lim= (int) (tail-singleCapacity*i+1);
            byteBuffer.limit(lim);
            ByteBuffer slice = byteBuffer.slice();
            compositeByteBuffer.addByteBuffers(slice);
            i++;
            begin+=slice.capacity();
        }
        return compositeByteBuffer;
    }

    public byte get() {
        ByteBuffer curBuffer = byteBuffers.get(this.index);
        byte b = curBuffer.get();
        if(curBuffer.position()==curBuffer.capacity()) this.index++;
        return b;
    }

    public void addByteBuffers(ByteBuffer byteBuffer){
        byteBuffers.add(byteBuffer);
        capacity+=byteBuffer.capacity();
    }

    public long capacity(){return this.capacity;}

    public long position(){return singleCapacity*index+byteBuffers.get(index).position();}

    public long getSingleCapacity(){return this.singleCapacity;}
}

