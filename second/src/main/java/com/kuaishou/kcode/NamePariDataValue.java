package com.kuaishou.kcode;

//服务粒度聚合后的P99和SR
public class NamePariDataValue {

    private int P99;

    private double SR;

    public NamePariDataValue(int p99, double SR) {
        P99 = p99;
        this.SR = SR;
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
        return "NamePariDataValue{" +
                "P99=" + P99 +
                ", SR=" + SR +
                '}';
    }
}
