package com.yihao.hadoopStudy.Po;

import com.google.gson.JsonParser;
import com.yihao.hadoopStudy.config.Config;


/**
 * Created by jzy on 2018/10/30.
 */
public class UserClickLog {
    private String mid;  // 7
    private String uid;  // 9

    private String osKey; //5

    private String algGroup;

    private JsonParser jsonParser = new JsonParser();

    public void parse(String value) throws ClickErrorException{
        clear();
        String[] split = value.split(Config.DEFAULT_DELIMITER);
        if (split.length<13) {
            throw new ClickErrorException("Input Format Error data length is not enough");
        }
        String mid = split[7];
        String uid = split[9];
        String osKey = split[5];
        String algGroup = split[13];


        setMid(mid);
        setUid(uid);
        setOsKey(osKey);
        setAlgGroup(algGroup);

    }

    private void clear() {
        setMid(null);
        setUid(null);
        setOsKey(null);
        setAlgGroup(null);
    }

    public String getOsKey() {
        return osKey;
    }

    public void setOsKey(String osKey) {
        this.osKey = osKey;
    }

    public String getAlgGroup() {
        return algGroup;
    }

    public void setAlgGroup(String algGroup) {
        this.algGroup = algGroup;
    }


    public static class ClickErrorException extends Exception {
        public ClickErrorException(String message) {
            super(message);
        }
    }

    public static class ClickWarningException extends Exception {
        public ClickWarningException(String message) {
            super(message);
        }
    }




    public String getMid() {
        return mid;
    }

    public void setMid(String mid) {
        this.mid = mid;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }


}
