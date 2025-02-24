package org.wso2.carbon.connector.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang.StringUtils;

import java.lang.reflect.Type;
import java.util.Map;

public class Utils {

    protected static final  Gson gson = new GsonBuilder().create();

    public static <T> T fromJson(String json, Type type) {
        return gson.fromJson(json, type);
    }
    public static  String toJson(Object object){
        return gson.toJson(object);
    }

    public static void addDataToMapFromJsonString(String jsonString, Map<String, String> metadataMap) {
        Map<String, String> map = Utils.fromJson(jsonString, Map.class);
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (StringUtils.isNotEmpty(entry.getValue().toString())) {
                metadataMap.put(entry.getKey().toString(), entry.getValue().toString());
            }
        }
    }


}
