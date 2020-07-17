package com.kuaishou.kcode;

/**
 * @author 杜科
 * @description 简单动态字符串，非线程安全。采取类似buffer的设计，使其成为一个可以方便重用的StringBuilder
 * @contact AllenDuke@163.com
 * @date 2020/6/9
 */
public class SDS implements Comparable<SDS>{

    private int readPosition;//下一个要读的下标

    private int writePositon;//下一个要写的下标

    private int capacity;//char数组大小

    private char[] chars;

    private int hashcode=0;

    public SDS(){
        this.capacity=20;
        this.chars=new char[20];
    }

    public SDS(int capacity){
        this.capacity=capacity;
        this.chars=new char[capacity];
    }

    public SDS(String s){
        this.capacity=s.length();
        this.chars=new char[this.capacity];
        for(int i=0;i<this.capacity;i++){
            this.chars[i]=s.charAt(i);
        }
    }

    public SDS append(char ch){
        if(writePositon==capacity){
            int oldCapacity=capacity;
            int newCapacity=capacity<<1;
            char[] newChars=new char[newCapacity];
            System.arraycopy(chars,0,newChars,0,oldCapacity);//todo Arrays.copyOf()性能更好？
            this.chars=newChars;
        }
        this.chars[writePositon++]=ch;
        return this;
    }

    public char charAt(int i){
        return this.chars[i];
    }

    public SDS setCharAt(int i, char ch){
        this.chars[i]=ch;
        return this;
    }

    public SDS clear(){
        this.writePositon=0;
        this.readPosition=0;
        this.hashcode=0;
        return this;
    }

    @Override
    public int compareTo(SDS sds){
        if(this.writePositon<sds.writePositon) return -1;
        if(this.writePositon>sds.writePositon) return 1;
        for(int i=0;i<writePositon;i++){
            if(chars[i]<sds.charAt(i)) return -1;
            if(chars[i]>sds.charAt(i)) return 1;
        }
        return 0;
    }


    @Override
    public int hashCode() {
        if(hashcode!=0) return hashcode;
        for(int i=0;i<writePositon;i++) hashcode=hashcode*31+chars[i];//与String的hashcode生成方法保持一致
        return hashcode;
    }

    @Override
    public boolean equals(Object obj) {
        if(this==obj) return true;
        if(obj.hashCode()!=this.hashcode) return false;
        if(!(obj instanceof SDS)) return false;
        SDS sds= (SDS) obj;
        if(sds.writePositon!=this.writePositon) return false;
        for(int i=0;i<this.writePositon;i++){
            if(sds.charAt(i)!=this.chars[i]) return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return new String(chars,0,writePositon);
    }
}
