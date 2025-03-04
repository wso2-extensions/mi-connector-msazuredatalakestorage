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

import java.util.HashMap;

/**
 * Utility class to handle common operations.
 */
public class Utils {

    protected static final Gson gson = new GsonBuilder().create();

    public static String toJson(Object object) {

        return gson.toJson(object);
    }

    /**
     * Add data to a map from a JSON string.
     *
     * @param arrayString Array string
     * @param metadataMap Map to add data
     */
    public static void addDataToMapFromJsonString(String arrayString, HashMap<String, String> metadataMap) {

        if (StringUtils.isEmpty(arrayString)) {
            return;
        }
        arrayString = arrayString.substring(1, arrayString.length() - 2);

        String[] entries = arrayString.split("],\\s*\\[");
        for (String entry : entries) {
            entry = entry.replace("[", "").replace("]", "");
            String[] pair = entry.split(",");

            if (pair.length == 2) {
                String key = pair[0].trim().replace("\"", "");
                String value = pair[1].trim().replace("\"", "");

                metadataMap.put(key, value);
            }
        }

    }

}
