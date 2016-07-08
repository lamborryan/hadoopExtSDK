package com.lamborryan.hive.UDF;

import org.apache.hadoop.hive.ql.exec.Description;

import java.text.ParseException;

/**
 * Created by chengfengruan on 16/7/7.
 */

@Description(
        name = "last_use_period",
        value = "_FUNC_(x, dateTimeFomat=\"yyyy-MM-dd HH:mm:ss\",) - Returns last use period of time to now ")
public class LastUsePeriodUDF extends DatetimePeriodUDF {

    public String evaluate(String registeTime){
        return evaluate(registeTime, DatetimePeriodUDF.defaultDateTimeFormat);
    }

    public String evaluate(String registeTime, String dateTimeFomat){
        if (registeTime == null){
            return null;
        }
        int deltaDate = 0;
        try {
            deltaDate = deltaDate(registeTime, dateTimeFomat);
        } catch (ParseException e) {
            return null;
        }

        if ( deltaDate <= 3){
            return "最近3天";
        }else if (deltaDate <= 7){
            return "最近1周";
        }else if (deltaDate <= 14){
            return "最近2周";
        }else if (deltaDate <= 30){
            return "最近1月";
        }else if (deltaDate <= 182){
            return "最近半年";
        }else if (deltaDate <= 365){
            return "最近1年";
        }else {
            return "1年以外";
        }
    }
}
