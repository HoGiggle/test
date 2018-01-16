package com.util;

import java.io.Serializable;

/**
 * Created by jjhu on 2017/1/16.
 */
public class JavaUtils implements Serializable{
    public static int hashEncodeUid(String uid) {
        String uidMd5Result = MD5.getMD5Code(uid);
        long preTenMd5ToDecimal = Long.parseLong(uidMd5Result.substring(0, 10), 16);
        int result = (int)(preTenMd5ToDecimal % Integer.MAX_VALUE);
        return result;
    }
}
