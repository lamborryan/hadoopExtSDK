package com.lamborryan.hive.UDF;

import com.lamborryan.hive.common.NRLCInfos;
import com.lamborryan.hive.common.NRLCParseUtil;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by chengfengruan on 16/7/7.
 */
@Description(
        name = "nrlc",
        value = "_FUNC_(x) - Returns Struct of nrlc infos")
public class GenericUDFNRLCParse extends GenericUDF{
    private StringObjectInspector key;
    public static String[] years = new String[]{
            "鼠","牛","虎","兔",
            "龙","蛇","马","羊",
            "猴","鸡","狗","猪"
    };
    private transient Object[] ret;
    private int field_num = 0;
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        assert (arguments.length == 1);
        ObjectInspector argument = arguments[0];
        if(argument.getCategory() != ObjectInspector.Category.PRIMITIVE){
            throw new UDFArgumentException("function only support primitive type.");
        }

        ArrayList<String> fname = new ArrayList<String>();
        // 生日
        fname.add("birth_day");
        // 性别
        fname.add("sex");
        // 星座
        fname.add("constellation");
        // 生肖
        fname.add("c_zodiac");
        // 年龄段 10岁-20岁..
        fname.add("age_group");
        // 年代 80, 90 ..
        fname.add("generation");
        // 省、自治区、直辖市代码;
        fname.add("province");
        // 地级市、盟、自治州代码;
        fname.add("city");
        // 县、县级市、区代码;
        fname.add("county");

        ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();
        for(int i = 0; i<fname.size(); i++){
            foi.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        }

        field_num = fname.size();
        key = (StringObjectInspector) argument;
        ret = new Object[field_num];
        return ObjectInspectorFactory.getStandardStructObjectInspector(fname, foi);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        if (arguments.length < 1) return null;

        DeferredObject argumentObj = arguments[0];
        if (argumentObj == null || argumentObj.get() == null){
            return null;
        }
        String argument = key.getPrimitiveJavaObject(argumentObj.get()).trim();
        if (argument.length() != 18 && argument.length() != 15){
            return null;
        }
        NRLCInfos nrlcInfos = NRLCParseUtil.parse(argument);
        DateFormat fmt =new SimpleDateFormat("yyyyMMdd");
        Calendar cal =Calendar.getInstance();
        try {
            Date date = fmt.parse(nrlcInfos.birthDay);
            cal.setTime(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < field_num ; i++){
            ret[i] = null;
        }

        // 生日
        ret[0] = nrlcInfos.birthDay;
        // 性别
        String sex ;
        if (nrlcInfos.sex == 0 ){
            sex = "女";
        }else if(nrlcInfos.sex == 1){
            sex = "男";
        }else{
            sex = "未知";
        }
        ret[1] = sex;
        // 星座
        ret[2] = getConstellation(cal);
        // 生肖
        ret[3] = getCzodiac(cal);
        // 年龄段 10岁-20岁..
        ret[4] = getAgeGroup(cal);
        // 年代 80, 90 ..
        ret[5] = getGeneration(cal);
        // 省、自治区、直辖市代码;
        ret[6] = nrlcInfos.province;
        // 地级市、盟、自治州代码;
        ret[7] = nrlcInfos.city;
        // 县、县级市、区代码;
        ret[8] = nrlcInfos.county;

        return ret;
    }



    @Override
    public String getDisplayString(String[] children) {
        return "_FUNC_(x) - Returns Struct of nrlc infos";
    }

    public static  String getConstellation(Calendar cal){
        int month = cal.get(Calendar.MONTH) + 1;
        int day = cal.get(Calendar.DAY_OF_MONTH);
        String star = "";
        if (month == 1 && day >= 21 || month == 2 && day <= 19) {
            star = "水瓶座";
        }
        if (month == 2 && day >= 20 || month == 3 && day <= 20) {
            star = "双鱼座";
        }
        if (month == 3 && day >= 21 || month == 4 && day <= 20) {
            star = "白羊座";
        }
        if (month == 4 && day >= 21 || month == 5 && day <= 21) {
            star = "金牛座";
        }
        if (month == 5 && day >= 22 || month == 6 && day <= 21) {
            star = "双子座";
        }
        if (month == 6 && day >= 22 || month == 7 && day <= 22) {
            star = "巨蟹座";
        }
        if (month == 7 && day >= 23 || month == 8 && day <= 23) {
            star = "狮子座";
        }
        if (month == 8 && day >= 24 || month == 9 && day <= 23) {
            star = "处女座";
        }
        if (month == 9 && day >= 24 || month == 10 && day <= 23) {
            star = "天秤座";
        }
        if (month == 10 && day >= 24 || month == 11 && day <= 22) {
            star = "天蝎座";
        }
        if (month == 11 && day >= 23 || month == 12 && day <= 21) {
            star = "射手座";
        }
        if (month == 12 && day >= 22 || month == 1 && day <= 20) {
            star = "摩羯座";
        }
        return star;
    }

    public static String getCzodiac(Calendar cal){
        int year = cal.get(Calendar.YEAR);

        if (year < 1900){
            return "未知";
        }
        Integer start = 1900;
        return years[(year-start)%years.length];
    }

    public static  String getAgeGroup(Calendar cal){
        Calendar now = Calendar.getInstance();
        long age = (now.getTime().getTime() - cal.getTime().getTime()) / 1000 / (24*3600*365);
        String age_group ="其他";
        if(age < 20){
            age_group = "0-20";
        }else if (age >= 20 && age <= 25){
            age_group = "20-25";
        }else if(age >= 26 && age <= 30){
            age_group = "26-30";
        }else if(age >= 31 && age <= 35){
            age_group = "31-35";
        }else if(age >= 36 && age <= 40){
            age_group = "36-40";
        }else if(age >= 41 && age <=45){
            age_group = "41-45";
        }else if(age >= 46 && age <=50){
            age_group = "46-50";
        }else if(age >= 51 && age <= 55){
            age_group = "51-55";
        }else if(age >= 56 && age <= 60){
            age_group = "56-60";
        }
        return age_group;
    }

    public static String getGeneration(Calendar cal){
        int year = cal.get(Calendar.YEAR);
        return String.valueOf((year%100)/10*10);
    }
}
