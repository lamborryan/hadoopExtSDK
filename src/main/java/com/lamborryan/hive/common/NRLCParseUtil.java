package com.lamborryan.hive.common;

/**
 * Created by chengfengruan on 16/7/7.
 * 解析身份证信息
 */
public class NRLCParseUtil {

    public static NRLCInfos parse(String idCardNum){

        if (idCardNum.length() != 18 && idCardNum.length() != 15 ){
            return null;
        }
        NRLCInfos nrlcInfo = new NRLCInfos();
        nrlcInfo.birthDay = getBirthDay(idCardNum);
        nrlcInfo.city = getCity(idCardNum);
        nrlcInfo.county = getCounty(idCardNum);
        nrlcInfo.province = getProvince(idCardNum);
        nrlcInfo.idCardNumber = idCardNum;
        nrlcInfo.sex = getSex(idCardNum);
        return nrlcInfo;
    }

    public static String getBirthDay(String idCardNum){
        String birthDay = null;
        if (idCardNum.length() == 18){
            birthDay = idCardNum.substring(6,14);
        }else if (idCardNum.length() == 15){
            birthDay = new StringBuffer("19").append(idCardNum.substring(6,12)).toString();
        }
        return birthDay;
    }

    public static String getProvince(String idCardNum){
        return idCardNum.substring(0,2);
    }

    public static String getCity(String idCardNum){
        return idCardNum.substring(2,4);
    }

    public static String getCounty(String idCardNum){
        return idCardNum.substring(4,6);
    }

    public static int getSex(String idCardNum){
        int sex = -1;
        if (idCardNum.length() == 18){
            sex = Integer.valueOf(idCardNum.substring(16,17));
        }else if (idCardNum.length() == 15){
            sex =  Integer.valueOf(idCardNum.substring(14,15));
        }
        if (sex > 0){
            // 返回0为女 返回1为男
            if (sex % 2 == 0){
                return 0;
            }else {
                return 1;
            }
        }else{
            return -1;
        }
    }
}


