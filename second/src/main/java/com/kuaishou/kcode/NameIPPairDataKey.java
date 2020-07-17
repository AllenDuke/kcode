package com.kuaishou.kcode;

/**
 * @author 杜科
 * @description 以name和ip对作key
 * @contact AllenDuke@163.com
 * @date 2020/6/29
 */
public class NameIPPairDataKey {

    private String callerName;

    private String respName;

    private String callerIP;

    private String respIP;

    private int hashcode=0;

    private long nameKey;

    private long iPKey;

    public long getNameKey() {
        return nameKey;
    }

    public long getiPKey() {
        return iPKey;
    }

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
//        //比起原来的|，这里hash得更均匀，冲突的几率更低
        this.hashcode=(this.callerName.hashCode()^this.respName.hashCode()^this.callerIP.hashCode()^this.respIP.hashCode());
//        this.hashcode= Objects.hash(this.callerIP,this.respIP,this.callerName,this.respName);
        this.nameKey=(this.callerName.hashCode()<<32)|this.respName.hashCode();
        this.iPKey=(this.callerIP.hashCode()<<32)|this.respIP.hashCode();
        return this.hashcode;
    }

    @Override
    public boolean equals(Object obj) {
//        if(this==obj) return true; 大概率!= 减少一步运算
//        if(!(obj instanceof NameIPPairDataKey)) return false; 大概率相同
        NameIPPairDataKey key= (NameIPPairDataKey) obj;
        return this.nameKey==key.getNameKey()&&this.iPKey==key.getiPKey();
//        return this.callerIP.equals(key.getCallerIP())&&this.respIP.equals(key.getRespIP())
//                &&this.callerName.equals(key.getCallerName()) &&this.respName.equals(key.getRespName());
    }

    @Override
    public String toString() {
        return callerName+","+callerIP+","+respName+","+respIP;
    }
}
