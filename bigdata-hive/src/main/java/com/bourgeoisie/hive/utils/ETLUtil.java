package com.bourgeoisie.hive.utils;

public class ETLUtil {
    public static String parseString(String ori) {
        String[] splits = ori.split("\t");
        //每行数据至少9个
        if (splits.length < 9) return null;
        StringBuilder etlString = new StringBuilder();
        //去除category中每个类别多余的空格
        splits[3] = splits[3].replace(" ", "");
        for (int i = 0; i < splits.length; i++) {
            if (i < 9) {
                etlString.append(splits[i]).append("\t");
            } else {
                etlString.append(splits[i]).append("&");
            }
        }
        return etlString.toString().substring(0, etlString.length() - 1);
    }
}
