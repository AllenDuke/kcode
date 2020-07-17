package com.kuaishou.kcode;

import java.util.ArrayList;
import java.util.List;

/**
 * @author 杜科
 * @description 包含每一个经IP聚合后的调用信息
 * @contact AllenDuke@163.com
 * @date 2020/6/21
 */
public class InvokeInfo{

    private List<Integer> costs=new ArrayList<>();//调用耗时

    private int successCount;//成功的次数

    private int count;//总次数

    public List<Integer> getCosts() {
        return costs;
    }

    public void setCosts(List<Integer> costs) {
        this.costs = costs;
    }

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

    @Override
    public String toString() {
        return "InvokeInfo{" +
                "costs=" + costs +
                ", successCount=" + successCount +
                ", count=" + count +
                '}';
    }
}
