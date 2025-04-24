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
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;

/**
 * Utility class for common operations.
 */
public class Utils {

    /**
     * Add data to a map from a JSON string.
     *
     * @param arrayString Array string
     * @param metadataMap Map to add data
     */
    public static void addDataToMapFromArrayString(String arrayString, HashMap<String, String> metadataMap) {

        if (StringUtils.equals("[]", arrayString)) {
            return;
        }

        Gson gson = new Gson();
        String[][] array = gson.fromJson(arrayString, String[][].class);

        for (String[] pair : array) {
            if (pair != null && pair.length == 2 && pair[0] != null && pair[1] != null) {
                metadataMap.put(pair[0], pair[1]);
            }
        }

    }
}
