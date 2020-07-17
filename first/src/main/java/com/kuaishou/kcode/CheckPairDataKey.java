package com.kuaishou.kcode;

/**
 * @author 杜科
 * @description
 * @contact AllenDuke@163.com
 * @date 2020/6/29
 */
public class CheckPairDataKey {

    private String callerName;

    private String respName;

    private String callerIP;

    private String respIP;

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

    @Override
    public int hashCode() {
        if(hashcode!=0) return this.hashcode;
            //比起原来的|，这里hash得更均匀，冲突的几率更低
        else this.hashcode=(this.callerName.hashCode()^this.respName.hashCode()^this.callerIP.hashCode()^this.respIP.hashCode())*31;
        return this.hashcode;
    }

    @Override
    public boolean equals(Object obj) {
        if(!(obj instanceof CheckPairDataKey)) return false;
        CheckPairDataKey key= (CheckPairDataKey) obj;
        return this.callerIP.equals(key.getCallerIP())&&this.respIP.equals(key.getRespIP())
                &&this.callerName.equals(key.getCallerName()) &&this.respName.equals(key.getRespName());
    }

    @Override
    public String toString() {
        return "CheckPairDataKey{" +
                "callerName='" + callerName + '\'' +
                ", respName='" + respName + '\'' +
                ", callerIP='" + callerIP + '\'' +
                ", respIP='" + respIP + '\'' +
                ", hashcode=" + hashcode +
                '}';
    }
}
