package com.yihao.hadoopStudy.Po;

import com.google.gson.JsonParser;
import com.yihao.hadoopStudy.config.Config;


/**
 * Created by jzy on 2018/10/30.
 */
public class UserShowLog {
    private String mid;
    private String uid;

    private String osKey;

    private String algGroup;//15

    private JsonParser jsonParser = new JsonParser();

    public void parse(String value) throws ShowErrorException{
        clear();
        String[] split = value.split(Config.DEFAULT_DELIMITER);
        if (split.length<11) {
            throw new UserShowLog.ShowErrorException("Input Format Error data length is not enough");
        }
        String mid = split[8];
        String uid = split[11];
        String osKey = split[6];
        String algGroup = split[15];
//        String thirdExt = split[18];
        setMid(mid);
        setUid(uid);
        setOsKey(osKey);
        setAlgGroup(algGroup);
        //JsonObject jsonObject = (JsonObject) jsonParser.parse(thirdExt);
//        setThirdExt(thirdExt);
    }

    private void clear() {
        setMid(null);
        setUid(null);
        setOsKey(null);
        setAlgGroup(null);
    }

    public String getAlgGroup() {
        return algGroup;
    }

    public void setAlgGroup(String algGroup) {
        this.algGroup = algGroup;
    }

    public static class ShowErrorException extends Exception {
        public ShowErrorException(String message) {
            super(message);
        }
    }

    public static class ShowWarningExceptionn extends Exception {
        public ShowWarningExceptionn(String message) {
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



    public String getOsKey() {
        return osKey;
    }

    public void setOsKey(String osKey) {
        this.osKey = osKey;
    }
}
