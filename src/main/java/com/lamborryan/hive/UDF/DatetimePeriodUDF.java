package com.lamborryan.hive.UDF;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

@Description(
        name = "time_period",
        value = "_FUNC_(x, dateTimeFomat=\"yyyy-MM-dd HH:mm:ss\", period) - Returns register period of time to now ")
public class DatetimePeriodUDF extends UDF {

    // 当前时间
    public static long now = Calendar.getInstance().getTime().getTime();
    public static String defaultDateTimeFormat = "yyyy-MM-dd HH:mm:ss";
    public static int WEEK = 7;
    public static int  MONTH = 30;
    public static int YEAR = 365;

    public int deltaDate(String registeTime, String dateTimeFomat) throws ParseException {
        // 将注册时间转成时间戳
        DateFormat fmt =new SimpleDateFormat(dateTimeFomat);
        Date registeDate = fmt.parse(registeTime);
        long registeTimestamp = registeDate.getTime();
        long deltaDate = (now - registeTimestamp) / 1000 / 24 / 3600;
        return (int) deltaDate;
    }

    public int evaluate(String registeTime, String dateTimeFomat,  String period) throws ParseException {

        int deltaDate = deltaDate(registeTime, dateTimeFomat);
        int delta = 0;
        if ("month".equals(period.toLowerCase())){
            delta = deltaDate / MONTH;
        }else if ("week".equals(period.toLowerCase())){
            delta = deltaDate / WEEK;
        }else if ("year".equals(period.toLowerCase())){
            delta = deltaDate / YEAR;
        }else if ("day".equals(period.toLowerCase())){
            delta = deltaDate;
        }
        return delta;
    }
}
