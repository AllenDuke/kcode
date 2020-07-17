package com.kuaishou.kcode;

//主被调ip对一分钟聚合后的结果
public class NameIPPairData {
    private String callerName;

    private String respName;

    private String callerIP;

    private String respIP;

    private int P99;

    private double SR;

    private int hashcode=0;

    public String getCallerIP() {
        return callerIP;
    }

    public void setCallerIP(String callerIP) {
        this.callerIP = callerIP;
    }

    public String getRespIP() {
        return respIP;
    }

    public void setRespIP(String respIP) {
        this.respIP = respIP;
    }

    public String getCallerName() {
        return callerName;
    }

    public void setCallerName(String callerName) {
        this.callerName = callerName;
    }

    public String getRespName() {
        return respName;
    }

    public void setRespName(String respName) {
        this.respName = respName;
    }

    public int getP99() {
        return P99;
    }

    public void setP99(int p99) {
        P99 = p99;
    }

    public double getSR() {
        return SR;
    }

    public void setSR(double SR) {
        this.SR = SR;
    }

    @Override
    public String toString() {
        return callerName+","+callerIP+","+respName+","+respIP+","+SR+","+P99;
    }
}
