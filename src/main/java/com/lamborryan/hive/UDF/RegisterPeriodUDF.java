package com.lamborryan.hive.UDF;

import org.apache.hadoop.hive.ql.exec.Description;

import java.text.ParseException;

/**
 * Created by chengfengruan on 16/7/7.
 */

@Description(
        name = "register_period",
        value = "_FUNC_(x, dateTimeFomat=\"yyyy-MM-dd HH:mm:ss\",) - Returns register period of time to now ")
public class RegisterPeriodUDF extends DatetimePeriodUDF{

    public static String[] flags = new String[]{
            "小于1个月", "1个月", "2个月", "3个月", "4个月", "5个月", "6个月",
            "7个月", "8个月", "9个月", "10个月", "11个月", "12个月", "大于1年"};

    public String evaluate(String registeTime){
        return evaluate(registeTime, DatetimePeriodUDF.defaultDateTimeFormat);
    }

    public String evaluate(String registeTime, String dateTimeFomat) {
        if (registeTime == null){
            return null;
        }
        int deltaNum = 0;
        try {
            deltaNum = evaluate(registeTime, dateTimeFomat, "month");
        } catch (ParseException e) {
            return null;
        }
        if (deltaNum  <= 12){
            return flags[deltaNum];
        }else {
            return flags[13];
        }
    }
}
