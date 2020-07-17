package com.kuaishou.kcode;

/**
 * @author 杜科
 * @description 简单动态字符串，非线程安全。采取类似buffer的设计，使其成为一个可以方便重用的StringBuilder
 * @contact AllenDuke@163.com
 * @date 2020/6/9
 */
public class SDS implements Comparable<SDS>{

    private int writePosition;//下一个要写的下标

    private int capacity;//char数组大小

    private char[] chars;

    private int hashcode=0;

    public SDS(){
        this.capacity=40;
        this.chars=new char[40];
    }

    public SDS(int capacity){
        this.capacity=capacity;
        this.chars=new char[capacity];
    }

    /**
     *在使用sds时，尽量设定好最大容量，以减少扩容判断
     */

    public SDS append(char ch){
//        if(writePosition==capacity) grow();
        this.chars[writePosition++]=ch;
        return this;
    }

    public SDS append(String s){
//        if((this.capacity-this.writePosition)<s.length()) grow();//先一次判断扩容
        for(int i=0;i<s.length();i++){
            this.chars[writePosition++]=s.charAt(i);
        }
        return this;
    }

    public SDS append(SDS sds){
//        if((this.capacity-this.writePosition)<sds.length()) grow();//先一次判断扩容
        for(int i=0;i<sds.length();i++){
            this.chars[writePosition++]=sds.charAt(i);
        }
        return this;
    }

    private void grow(){
        int oldCapacity=capacity;
        int newCapacity=capacity<<1;
        char[] newChars=new char[newCapacity];
        System.arraycopy(chars,0,newChars,0,oldCapacity);
        capacity=newCapacity;
        this.chars=newChars;
    }

    public char charAt(int i){
        return this.chars[i];
    }

    public SDS setCharAt(int i, char ch){
        this.chars[i]=ch;
        return this;
    }

    public SDS clear(){
        this.writePosition=0;
        this.hashcode=0;
        return this;
    }

    public int length(){
        return this.writePosition;
    }

    @Override
    public int compareTo(SDS sds){
        if(this.writePosition<sds.writePosition) return -1;
        if(this.writePosition>sds.writePosition) return 1;
        for(int i=0;i<writePosition;i++){
            if(chars[i]<sds.charAt(i)) return -1;
            if(chars[i]>sds.charAt(i)) return 1;
        }
        return 0;
    }


    @Override
    public int hashCode() {
        if(hashcode!=0) return hashcode;
        for(int i=0;i<writePosition;i++) hashcode=hashcode*31+chars[i];//与String的hashcode生成方法保持一致
        return hashcode;
    }

    @Override
    public boolean equals(Object obj) {
        if(this==obj) return true;
        if(obj.hashCode()!=this.hashcode) return false;
        if(!(obj instanceof SDS)) return false;
        SDS sds= (SDS) obj;
        if(sds.writePosition!=this.writePosition) return false;
        for(int i=0;i<this.writePosition;i++){
            if(sds.charAt(i)!=this.chars[i]) return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return new String(chars,0,writePosition);
    }

}
