package com.input.cheat;

import org.apache.commons.math3.ml.distance.DistanceMeasure;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by jjhu on 2017/2/17.
 */
public class JaccardSim implements DistanceMeasure {
    @Override
    public double compute(double[] a, double[] b) {
        if (a == null || b == null){
            return 0d;
        }

        //O(n)
        Set<Double> set = new HashSet<>();
        if (a.length >= b.length){
            for (Double item : a){
                set.add(item);
            }
            return jaccard(b, set);
        }else {
            for (Double item : b){
                set.add(item);
            }
            return jaccard(a, set);
        }
    }

    private double jaccard(double[] a, Set<Double> b){
        int intersectSize = 0, unionSize = a.length + b.size();
        for (Double item : a){
            if (b.contains(item)){
                intersectSize++;
                unionSize--;
            }
        }

        System.out.println("Jaccard coming!");

        if (intersectSize > 0){
            return 1d - intersectSize * 1.0d / unionSize;
        }
        return 1d;
    }
}
