package com.lamborryan.hive.UDF;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by chengfengruan on 16/6/23.
 */

@Description(
        name = "regex_to_named_struct",
        value = "_FUNC_(x, reg, name1, name2, name3....) - Returns Struct of str by regex")

public class GenericUDFRegstrToStruct extends GenericUDF {
    private transient Object[] ret;
    private StringObjectInspector key;
    private StringObjectInspector reg;
    private int keyNum = 0;
    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        assert (objectInspectors.length >= 3);
        for (ObjectInspector oi : objectInspectors){
            if(oi.getCategory() != ObjectInspector.Category.PRIMITIVE){
                throw new UDFArgumentException("function only support primitive type.");
            }
        }

        ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();
        ArrayList<String> fname = new ArrayList<String>();
        int numFields = objectInspectors.length;
        for(int i = 2; i<numFields; i++){
            foi.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
            ConstantObjectInspector constantOI = (ConstantObjectInspector)objectInspectors[i];
            fname.add(constantOI.getWritableConstantValue().toString());
        }
        key = (StringObjectInspector) objectInspectors[0];
        reg = (StringObjectInspector) objectInspectors[1];
        keyNum = numFields -2;
        ret = new Object[keyNum];
        return ObjectInspectorFactory.getStandardStructObjectInspector(fname, foi);
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {

        if (deferredObjects.length < 1) return null;

        // 清空上一条记录
        for (int i = 0; i < keyNum; i++){
            ret[i] = null ;
        }

        try {
            ArrayList<String> listStr = parse(key.getPrimitiveJavaObject(deferredObjects[0].get()),
                    reg.getPrimitiveJavaObject(deferredObjects[1].get()));
            for (int i = 0; i < Math.min(keyNum, listStr.size()); i++){
                ret[i] = listStr.get(i);
            }
            return ret;
        } catch (Exception e){
            System.out.println(e.getMessage());
            return null;
        }
    }

    public  ArrayList<String> parse(String str,String regEx){
        ArrayList<String> parsedList = new ArrayList<String>();
        Pattern p = Pattern.compile(regEx);
        Matcher m = p.matcher(str);
        if (m.matches()){
            int count = m.groupCount();
            for(int i = 0; i < count; i++) {
                String group = m.group(i + 1);
                if( group.length() > 0) {
                    parsedList.add(group);
                }
            }
        }
        return parsedList;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "_FUNC_(x, reg, names,....) - Returns Struct of str by regex";
    }

    public static void main(String[] args) {

//        select regext_to_named_struct(user_agent,'([^()]*) \\(([^\;]*)\;([^\;]*)\;([^\;]*)\;([^\;]*)\;([^\;]*)\\)', 'test1','test2','t3','t4','t5','t6') f
//        rom ods.ods_bxs_service_requestlog where pt=20160604 limit 10;

        //String reg = "([^()]*) \\(([^;]*);([^;]*);([^;]*);([^;]*);?([^;]*)\\)";

        String reg = "([^()]*)\\(([^;]*)[;]?([^;]*)[;]?([^;]*)[;]?([^;]*)[;]?([^;]*)\\)";
        //String str = "bxs_a (m1 metal;android5.1;2.0.0;1920.0x1080.0;postition_xxxx)";
        String str = "bxs_i(iPhone; 9.3.2; 2.0.0; 375x667; position_110.1334642701214_22.58369232095267)";
        //String str = "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)";
        Pattern p = Pattern.compile(reg);
        Matcher m = p.matcher(str);
        if (m.matches()) {
            for (int i = 1; i <= m.groupCount(); i++) {
                String a = m.group(i);
                if (a.equals("")) {
                    System.out.println("null");
                }else {
                    System.out.println(a.trim());
                }
            }
            System.out.println(m.groupCount());
        }
    }
}
