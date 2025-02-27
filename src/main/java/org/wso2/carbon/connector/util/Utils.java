/*
 *  Copyright (c) 2025, WSO2 LLC. (https://www.wso2.com).
 *
 *  WSO2 LLC. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.wso2.carbon.connector.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang.StringUtils;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class to handle common operations.
 */
public class Utils {

    protected static final Gson gson = new GsonBuilder().create();

    public static <T> T fromJson(String json, Type type) {

        return gson.fromJson(json, type);
    }

    public static String toJson(Object object) {

        return gson.toJson(object);
    }

    public static void addDataToMapFromJsonString(String jsonString, HashMap<String, String> metadataMap) {
        if (StringUtils.isEmpty(jsonString)) {
            return;
        }
        Map<String, String> map = Utils.fromJson(jsonString.replace("'", ""), Map.class);
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (StringUtils.isNotEmpty(entry.getValue().toString())) {
                metadataMap.put(entry.getKey().toString(), entry.getValue().toString());
            }
        }
    }

}
