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

import org.apache.synapse.MessageContext;
import org.wso2.carbon.connector.core.AbstractConnector;
import org.wso2.carbon.connector.exceptions.InvalidConfigurationException;



/**
 * This class contain required util methods for to Azure Storage connector.
 */
public class AzureUtil extends AbstractConnector {

    @Override
    public void connect(MessageContext messageContext) {
    }

//    public static String getStorageConnectionString(String accountName, String accountKey, String protocol) {
//        return AzureConstants.PROTOCOL_KEY_PARAM + protocol + AzureConstants.SEMICOLON +
//               AzureConstants.ACCOUNT_NAME_PARAM + accountName + AzureConstants.SEMICOLON
//               + AzureConstants.ACCOUNT_KEY_PARAM + accountKey;
//    }
//

}
