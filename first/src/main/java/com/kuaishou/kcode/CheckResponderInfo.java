package com.kuaishou.kcode;

/**
 * @author 杜科
 * @description 记录某一秒responder的调用成功率相关的信息
 * @contact AllenDuke@163.com
 * @date 2020/6/21
 */
public class CheckResponderInfo {

    private int successCount;//成功的次数

    private int count;//总次数

    private double successRate;//成功率

    public int getSuccessCount() {
        return successCount;
    }

    public void setSuccessCount(int successCount) {
        this.successCount = successCount;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public double getSuccessRate() {
        return successRate;
    }

    public void setSuccessRate(double successRate) {
        this.successRate = successRate;
    }

    @Override
    public String toString() {
        return "CheckResponderInfo{" +
                "successCount=" + successCount +
                ", count=" + count +
                ", successRate=" + successRate +
                '}';
    }
}
