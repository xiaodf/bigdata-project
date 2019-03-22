package com.bigdata.hbase.rest;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

/**
 * Created by xdf on 2019/2/28.
 */
public class HBaseRestDemo {
    public static void main(String[] args) {

    }

    /**
     * 获取表list(get请求)：
     * @param acceptInfo
     * @return
     */
    public static String getTableList(String acceptInfo){
        String uriAPI = "http://example.com:8080/";
        String result = "";
        HttpGet httpRequst = new HttpGet(uriAPI);
        try {
            httpRequst.setHeader("accept", acceptInfo);
            HttpClient client = HttpClientBuilder.create().build();
            HttpResponse httpResponse = client.execute(httpRequst);
            // 其中HttpGet是HttpUriRequst的子类
            int statusCode = httpResponse.getStatusLine().getStatusCode();
            if (statusCode == 200 || statusCode == 403) {
                HttpEntity httpEntity = httpResponse.getEntity();
                try {
                    result = EntityUtils.toString(httpEntity);// 取出应答字符串
                } catch (IOException e) {
                    e.printStackTrace();
                }
                // 一般来说都要删除多余的字符
                // 去掉返回结果中的"\r"字符，否则会在结果字符串后面显示一个小方格
                // result.replaceAll("\r", "");
            } else {
                httpRequst.abort();
                result = "异常的返回码:"+statusCode;
            }
        } catch (ClientProtocolException e) {
            e.printStackTrace();
            result = e.getMessage().toString();
        } catch (IOException e) {
            e.printStackTrace();
            result = e.getMessage().toString();
        }
        return result;
    }

    /**
     * 获取表结构(get请求)
     * @param tableName
     * @param acceptInfo
     * @return
     */
    public static String getSchemaInfo(String tableName, String acceptInfo) {
        String uriAPI = "http://example.com:8080/" + tableName + "/schema";
        String result = "";
        HttpGet httpRequst = new HttpGet(uriAPI);
        try {
            httpRequst.setHeader("accept", acceptInfo);
            HttpClient client = HttpClientBuilder.create().build();
            HttpResponse httpResponse = client.execute(httpRequst);
            // 其中HttpGet是HttpUriRequst的子类
            int statusCode = httpResponse.getStatusLine().getStatusCode();
            if(statusCode == 200 || statusCode == 403) {
                HttpEntity httpEntity = httpResponse.getEntity();
                result = EntityUtils.toString(httpEntity);// 取出应答字符串
                // 一般来说都要删除多余的字符
                // 去掉返回结果中的"\r"字符，否则会在结果字符串后面显示一个小方格
                // result.replaceAll("\r", "");
            } else {
                httpRequst.abort();
                result = "异常的返回码:"+statusCode;
            }
        } catch (ClientProtocolException e) {
            e.printStackTrace();
            result = e.getMessage().toString();
        } catch (IOException e) {
            e.printStackTrace();
            result = e.getMessage().toString();
        }
        return result;
    }

    /**
     * 创建一张表(post请求)
     * @param newTableName
     * @param jsonOrXmlStr
     * @param jsonOrXml
     * @return
     */
    public static String createHtable(String newTableName, String jsonOrXmlStr,String jsonOrXml) {
        String uriAPI = "http://example.com:8080/" + newTableName + "/schema";
        String result = "";
        HttpPost httpRequst = new HttpPost(uriAPI);
        try {
            StringEntity s = new StringEntity(jsonOrXmlStr.toString());
            httpRequst.setHeader("accept", jsonOrXml);
            httpRequst.setHeader("Content-Type", jsonOrXml);
            httpRequst.setEntity(s);
            HttpClient client = HttpClientBuilder.create().build();
            HttpResponse httpResponse = client.execute(httpRequst);
            // 其中HttpGet是HttpUriRequst的子类
            int statusCode = httpResponse.getStatusLine().getStatusCode();
            if (statusCode == 200||statusCode == 201) {
                HttpEntity httpEntity = httpResponse.getEntity();
                result = "code=>"+statusCode+":"+EntityUtils.toString(httpEntity);// 取出应答字符串
                // 一般来说都要删除多余的字符
                // 去掉返回结果中的"\r"字符，否则会在结果字符串后面显示一个小方格
                // result.replaceAll("\r", "");
            } else {
                httpRequst.abort();
                result = "没有返回正确的状态码，请仔细检查请求表名";
            }
        } catch (ClientProtocolException e) {
            e.printStackTrace();
            result = e.getMessage().toString();
        } catch (IOException e) {
            e.printStackTrace();
            result = e.getMessage().toString();
        }
        return result;
    }

    /**
     * 删除一张表(delete请求)
     * @param deteleTableName
     * @param jsonOrXml
     * @return
     */
    public static String deleteHtable(String deteleTableName,String jsonOrXml) {
        String uriAPI = "http://121.199.28.106:8080/" + deteleTableName + "/schema";
        String result = "";
        HttpDelete httpRequst = new HttpDelete(uriAPI);
        try {
            httpRequst.setHeader("accept", jsonOrXml);
            httpRequst.setHeader("Content-Type", jsonOrXml);
            HttpClient client = HttpClientBuilder.create().build();
            HttpResponse httpResponse = client.execute(httpRequst);
            // 其中HttpGet是HttpUriRequst的子类
            int statusCode = httpResponse.getStatusLine().getStatusCode();
            if (statusCode == 200) {
                HttpEntity httpEntity = httpResponse.getEntity();
                result = "code=>"+statusCode+":"+EntityUtils.toString(httpEntity);// 取出应答字符串
                // 一般来说都要删除多余的字符
                // 去掉返回结果中的"\r"字符，否则会在结果字符串后面显示一个小方格
                // result.replaceAll("\r", "");
            } else {
                httpRequst.abort();
                result = "没有返回正确的状态码，请仔细检查请求表名";
            }
        } catch (ClientProtocolException e) {
            e.printStackTrace();
            result = e.getMessage().toString();
        } catch (IOException e) {
            e.printStackTrace();
            result = e.getMessage().toString();
        }
        return result;
    }

    /**
     * 写入一条rowkey数据(put请求)
     * @param tableName
     * @param jsonStr
     * @return
     */
    public static String writeRowInTableByJson(String tableName, String jsonStr) {
        String uriAPI = "http://121.199.28.106:8080/" + tableName + "/fakerow";
        StringBuilder result = new StringBuilder();
        HttpPut put = new HttpPut(uriAPI);
        try {
            put.addHeader("Accept", "application/json");
            put.addHeader("Content-Type", "application/json");
            // JSONObject jsonObject = JSONObject.fromObject(jsonStr);
            StringEntity input = null;
            try {
                input = new StringEntity(jsonStr);
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            put.setEntity(input);
            HttpClient client = HttpClientBuilder.create().build();
            HttpResponse httpResponse = client.execute(put);
            int status = httpResponse.getStatusLine().getStatusCode();
            if ( status != 200) {
                throw new RuntimeException("Failed : HTTP error code : " + httpResponse.getStatusLine().getStatusCode());
            }
            BufferedReader br = new BufferedReader(new InputStreamReader((httpResponse.getEntity().getContent())));
            String output;
            while ((output = br.readLine()) != null) {
                result.append(output);
            }
            result.append("-code:"+status);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return result.toString();
    }

    /**
     * 获取一条rowkey信息(get请求)
     * @param tableName
     * @param rowkey
     * @param paramInfo
     * @param xmlOrJson
     * @return
     */
    public static String getRowKey(String tableName, String rowkey,String paramInfo, String xmlOrJson) {
        String uriAPI = "http://121.199.28.106:8080/" + tableName + "/" + rowkey+"/info:"+paramInfo;
        String result = "";
        HttpGet getR = new HttpGet(uriAPI);
        try {
            getR.setHeader("accept", xmlOrJson);
            HttpClient client = HttpClientBuilder.create().build();
            HttpResponse httpResponse = client.execute(getR);
            // 其中HttpGet是HttpUriRequst的子类
            int statusCode = httpResponse.getStatusLine().getStatusCode();
            if (statusCode == 200 || statusCode == 403) {
                HttpEntity httpEntity = httpResponse.getEntity();
                result = EntityUtils.toString(httpEntity);// 取出应答字符串
                // 一般来说都要删除多余的字符
                // 去掉返回结果中的"\r"字符，否则会在结果字符串后面显示一个小方格
                // result.replaceAll("\r", "");
            } else {
                getR.abort();
                result = "没有返回正确的状态码，请仔细检查请求表名及参数格式！";
            }
        } catch (ClientProtocolException e) {
            e.printStackTrace();
            result = e.getMessage().toString();
        } catch (IOException e) {
            e.printStackTrace();
            result = e.getMessage().toString();
        }
        return result;
    }

    /**
     *  scan 扫描 (get请求)
     * @param tableName
     * @param startRowkey
     * @param endRowkey
     * @param params
     * @param xmlOrJson
     * @return
     */
    public static String scan(String tableName,String startRowkey,String endRowkey,String[] params,String xmlOrJson){
        String uriAPI = "http://121.199.28.106:8080/" + tableName + "/*?startrow=" + startRowkey+"&endrow="+endRowkey;
        String result = "";
        HttpGet getR = new HttpGet(uriAPI);
        try {
            getR.setHeader("accept", xmlOrJson);
            HttpClient client = HttpClientBuilder.create().build();
            HttpResponse httpResponse = client.execute(getR);
            // 其中HttpGet是HttpUriRequst的子类
            int statusCode = httpResponse.getStatusLine().getStatusCode();
            if (statusCode == 200 || statusCode == 403) {
                HttpEntity httpEntity = httpResponse.getEntity();
                result = EntityUtils.toString(httpEntity);// 取出应答字符串
                // 一般来说都要删除多余的字符
                // 去掉返回结果中的"\r"字符，否则会在结果字符串后面显示一个小方格
                // result.replaceAll("\r", "");
            } else {
                getR.abort();
                result = "没有返回正确的状态码，请仔细检查请求表名及参数格式！";
            }
        } catch (ClientProtocolException e) {
            e.printStackTrace();
            result = e.getMessage().toString();
        } catch (IOException e) {
            e.printStackTrace();
            result = e.getMessage().toString();
        }
        return result;
    }
}
