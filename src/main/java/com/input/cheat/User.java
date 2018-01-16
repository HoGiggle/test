package com.input.cheat;

import org.apache.commons.math3.ml.clustering.Clusterable;

import java.io.Serializable;

/**
 * Created by jjhu on 2017/2/17.
 */
public class User implements Clusterable, Serializable {
    private String uid;
    private double[] indexs;

    public User(String uid, double[] indexs) {
        this.uid = uid;
        this.indexs = indexs;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public double[] getIndexs() {
        return indexs;
    }

    public void setIndexs(double[] indexs) {
        this.indexs = indexs;
    }

    @Override
    public double[] getPoint() {
        return indexs;
    }
}
