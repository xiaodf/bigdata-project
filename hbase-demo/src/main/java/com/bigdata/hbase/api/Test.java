package com.bigdata.hbase.api;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by dongfangxiao on 2019/3/14.
 */
public class Test {
    public static void main(String[] args) {
        Test md5 = new Test();
        String ss = md5.encryption("863184031478397");
        String str = "228de1127b7c0eef5ef5daac22f6a3ee";
        System.out.println(ss);
        System.out.println(ss.equalsIgnoreCase(str));
    }

    public String encryption(String OrderNo) {
       // String result = OrderNo+"354039456123789"+"andriod";
        String result = OrderNo;
        String re_md5 = new String();
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(result.getBytes());
            byte b[] = md.digest();

            int i;

            StringBuffer buf = new StringBuffer("");
            for (int offset = 0; offset < b.length; offset++) {
                i = b[offset];
                if (i < 0)
                    i += 256;
                if (i < 16)
                    buf.append("0");
                buf.append(Integer.toHexString(i));
            }

            re_md5 = buf.toString();

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        //return re_md5.toUpperCase();
        return re_md5;
    }

}
