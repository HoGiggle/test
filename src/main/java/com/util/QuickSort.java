package com.util;

/**
 * Created by jjhu on 2018/3/6.
 */
public class QuickSort {
    static void quicksort(int n[], int left, int right) {
        int dp;
        if (left < right) {
            dp = partition(n, left, right);
            quicksort(n, left, dp - 1);
            quicksort(n, dp + 1, right);
        }
    }

    static int partition(int n[], int left, int right) {
        int pivot = n[left];
        while (left < right) {
            while (left < right && n[right] >= pivot)
                right--;
            if (left < right)
                n[left++] = n[right];
            while (left < right && n[left] <= pivot)
                left++;
            if (left < right)
                n[right--] = n[left];
        }
        n[left] = pivot;
        return left;
    }

    public static void main(String[] args) {
        int[] in = new int[]{1, 3, 2, 4, 5, 0, 7, 9, 9, 9, 9,9, 9,9, 9,9, 9};
        quicksort(in, 0, in.length - 1);
        for (int i = 0; i < in.length; i++) System.out.println(in[i]);
    }
}
