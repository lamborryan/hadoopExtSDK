package com.lamborryan.hive.UDF;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by chengfengruan on 16/7/7.
 *
 * 解析手机号码, 返回运营商
 */

@Description(
        name = "mobile_isp",
        value = "_FUNC_(x) - Returns ISP of mobile like ChinaMobile,ChinaUnicom,ChinaTelecom and so on")
public class MobileParseUDF extends UDF{

    // 移动
    public static String[] ChinaMobile = new String[]{"135","136","137","138","139","147","150","151",
                                                      "152","157","158","159","178","182","183","184",
                                                      "187","188"};
    // 联通
    public static String[] ChinaUnicom = new String[]{"130","131","132","145","155","156","176","185",
                                                      "186"};
    // 电信
    public static String[] ChinaTelecom = new String[]{"133","153","177","173","180","181","189"};

    // 虚拟运营商
    public static String[] Virtual = new String[]{"170","171"};

    public static String MOBILE = "移动";

    public static String UNICOM = "联通";

    public static String TELECOM = "电信";

    public static String VIRTUAL = "虚拟商";

    public static String UNKNOW = "未知";

    public String evaluate(String mobile) {

        if ( mobile == null || mobile.trim().length() != 11){
            return null ;
        }

        String mobile_pre = mobile.substring(0,3);

        for(String flag : ChinaMobile){
            if (flag.equals(mobile_pre)) return MOBILE;
        }

        for(String flag : ChinaUnicom){
            if (flag.equals(mobile_pre)) return UNICOM;
        }

        for(String flag : ChinaTelecom){
            if (flag.equals(mobile_pre)) return TELECOM;
        }

        for(String flag : Virtual){
            if (flag.equals(mobile_pre)) return VIRTUAL;
        }

        return UNKNOW;
    }
}
