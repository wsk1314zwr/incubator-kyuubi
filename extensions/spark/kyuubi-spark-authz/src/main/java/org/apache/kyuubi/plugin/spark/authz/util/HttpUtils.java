/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kyuubi.plugin.spark.authz.util;

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * http utils
 */
public class HttpUtils {


    public static final Logger logger = LoggerFactory.getLogger(HttpUtils.class);

    /**
     * get http request content
     *
     * @param url url
     * @return http get request response content
     */
    public static String get(String url, Map<String, String> heads) {
        CloseableHttpClient httpclient = HttpClients.createDefault();

        HttpGet httpget = new HttpGet(url);
        RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(60 * 1000)
                .setConnectionRequestTimeout(60 * 1000)
                .setSocketTimeout(60 * 1000)
                .setRedirectsEnabled(true)
                .build();
        httpget.setConfig(requestConfig);
        if (Objects.nonNull(heads)) {
            heads.forEach(httpget::setHeader);
        }
        String responseContent = null;
        CloseableHttpResponse response = null;

        try {
            response = httpclient.execute(httpget);
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    responseContent = EntityUtils.toString(entity, StandardCharsets.UTF_8);
                } else {
                    logger.warn("http entity is null");
                }
            } else {
                String errMsg = String.format("http get:response status code is %d not 200, url = %s", response.getStatusLine().getStatusCode(), url);
                throw new RuntimeException(errMsg);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        } finally {
            try {
                if (response != null) {
                    EntityUtils.consume(response.getEntity());
                    response.close();
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }

            try {
                if (!httpget.isAborted()) {
                    httpget.releaseConnection();
                    httpget.abort();
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }

            try {
                httpclient.close();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
        return responseContent;
    }

    public static String post(String body, String url, Map<String, String> header) {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpPost httpPost = null;
        String responseContent = null;
        CloseableHttpResponse response = null;
        try {
            httpPost = new HttpPost(url);

            //设置超时时间
            RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(60 * 1000)
                    .setConnectionRequestTimeout(60 * 1000)
                    .setSocketTimeout(60 * 1000)
                    .setRedirectsEnabled(true)
                    .build();
            httpPost.setConfig(requestConfig);
            if (Objects.nonNull(header)) {
                header.forEach(httpPost::setHeader);
            }
            httpPost.setHeader("Content-Type", "application/json; charset=utf-8");
            httpPost.setHeader("Connection", "Close");
            String sessionId = getUuid();
            httpPost.setHeader("SessionId", sessionId);

            // 构建消息实体
            StringEntity entity = new StringEntity(body, StandardCharsets.UTF_8);
            entity.setContentEncoding("UTF-8");

            // 发送Json格式的数据请求
            entity.setContentType("application/json");
            httpPost.setEntity(entity);
            response = httpclient.execute(httpPost);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != HttpStatus.SC_OK) {
                String errMsg = String.format("http post:response status code is %d not 200, url = %s", statusCode, url);
                throw new RuntimeException(errMsg);
            } else {
                HttpEntity responseEntity = response.getEntity();
                if (responseEntity != null) {
                    responseContent = EntityUtils.toString(responseEntity, StandardCharsets.UTF_8);
                } else {
                    logger.warn("http entity is null");
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (response != null) {
                    EntityUtils.consume(response.getEntity());
                    response.close();
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }

            try {
                if (!httpPost.isAborted()) {
                    httpPost.releaseConnection();
                    httpPost.abort();
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }

            try {
                httpclient.close();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
        return responseContent;
    }

    public static String getUuid() {
        return UUID.randomUUID().toString().replace("-", "");
    }

}
