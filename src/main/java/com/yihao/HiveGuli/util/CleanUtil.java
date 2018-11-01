package com.yihao.HiveGuli.util;

import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by jzy on 2018/11/1.
 */
public class CleanUtil {
    public static String clean(String line) {

        // 9 : 1
        String[] split = line.split("\\t",10);
        // 字段小于9个直接清洗
        if (split.length < 10) {
            return null;
        }
        // 清除空格
        split[3] = split[3].replaceAll("\\s+", "");
        split[9] = split[9].replaceAll("\\t", "&");

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < split.length; i++) {
            sb.append(split[i]);
            if (i != split.length - 1) {
                sb.append("\t");
            }
        }
        return sb.toString();

    }

    public static void main(String[] args) {

        String clean = clean("LKh7zAJ4nwo\tTheReceptionist\t653\tEntertainment\t424\t13021\t4.34\t1305\t744\tDjdA-5oKYFQ\tNxTDlnOuybo\tc-8VuICzXtU\tDH56yrIO5nI\tW1Uo5DQTtzc\tE-3zXq_r4w0\t1TCeoRPg5dE\tyAr26YhuYNY\t2ZgXx72XmoE\t-7ClGo-YgZ0\tvmdPOOd6cxI\tKRHfMQqSHpk\tpIMpORZthYw\t1tUDzOp10pk\theqocRij5P0\t_XIuvoH6rUg\tLGVU5DsezE0\tuO2kj6_D8B4\txiDqywcDQRM\tuX81lMev6_o");
        FileWriter fileWriter=null;
        try {
            fileWriter = new FileWriter("1.txt");
            fileWriter.write(clean);
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


}
