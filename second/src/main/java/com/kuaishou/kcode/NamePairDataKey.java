package com.kuaishou.kcode;

//以name对作key
public class NamePairDataKey {

    private String callerName;

    private String respName;

    private int hashcode=0;

    private long nameKey;

    public long getNameKey() {
        return nameKey;
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
        this.hashcode=(this.callerName.hashCode()^this.respName.hashCode());
//        this.hashcode= Objects.hash(this.callerName,this.respName);
        this.nameKey=(this.callerName.hashCode()<<32)|this.respName.hashCode();
        return this.hashcode;
    }

    @Override
    public boolean equals(Object obj) {
//        if(this==obj) return true; 大概率!= 减少一步运算
//        if(!(obj instanceof NamePairDataKey)) return false; 大概率相同
        NamePairDataKey key= (NamePairDataKey) obj;
        return this.nameKey==key.getNameKey();
//        return this.callerName.equals(key.getCallerName()) &&this.respName.equals(key.getRespName());
    }

    @Override
    public String toString() {
        return callerName+","+respName;
    }
}
