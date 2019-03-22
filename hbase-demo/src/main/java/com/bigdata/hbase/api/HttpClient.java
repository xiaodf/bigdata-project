package com.bigdata.hbase.api;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

public class HttpClient {

    public static void main(String[] args) {
        getTest();
        //postTest();
    }

    public static void getTest() {
       //String geturl = "https://api.e.qq.com/oauth/token?client_id=1108202679&client_secret=LqM0BCSDsj62YwtI&grant_type=authorization_code&authorization_code=1766d35c574a31be2791e172711b5f70&redirect_uri=https://ss.sohu.com/datareport/guangdiantong/callback";
      // String geturl = "https://api.e.qq.com/oauth/token?client_id=1108202679&client_secret=LqM0BCSDsj62YwtI&grant_type=refresh_token&refresh_token=f2cebdc4475c09a989ebe2cd923668ed";
        String geturl = "http://ad.partner.gifshow.com/track/activate?event_type=7&event_time=1553062658689&callback=DHAJASALKFyk1uCKBYCyXp-iIDS-uHDd_a5SJ9Dbwkqv46dahahd87TW7hhkJkd";
        String json = doGet(geturl);
//        JSONObject result = (JSONObject) JSONObject.fromObject(json).get("data");
//        String access_token= result.getString("access_token");
        System.out.println(json);
    }

    public static void postTest() {
        //String json ="{\"client_id\":1108202679,\"client_secret\": \"LqM0BCSDsj62YwtI\",\"grant_type\": \"authorization_code\",\"authorization_code\": \"4136120abc29de0c8e47d8419cb36f41&state=\",\"redirect_uri\": \"https://ss.sohu.com\"}";
        //String json = "[{\"action_time\":1552900416,\"user_id\": \"7b1f3f8c4bb58e41d56beabeb2f4b38f\",\"os\": 2},{\"action_time\":1552900416,\"user_id\": \"7b1f3f8c4bb58e41d56beabeb2f4b38f\",\"os\": 1}]";
         String json ="{\"data\":[{\"action_time\":1552900416,\"user_id\": \"7b1f3f8c4bb58e41d56beabeb2f4b38f\",\"os\": 2},{\"action_time\":1552900416,\"user_id\": \"7b1f3f8c4bb58e41d56beabeb2f4b38f\",\"os\": 1}]}";
       // String json ="{\"data\":[{\"action_time\":1552900416,\"user_id\":\"7b1f3f8c4bb58e41d56beabeb2f4b38f\",\"os\":1}]}";
        JSONObject jsonParam = JSONObject.fromObject(json);
        String postURL = "http://localhost:8500/datareport/guangdiantong/reportretain";
        //String postURL = "http://10.18.94.41:9002/riskcontrol/deviceids?appKey=123-c4ca4238a0b923820dcc509a6f75849b";
        String result = doPost(postURL,jsonParam.toString()).toString();
        System.out.println(result);
    }


    public static String doGet(String httpurl) {
        HttpURLConnection connection = null;
        InputStream is = null;
        BufferedReader br = null;
        String result = null;// 返回结果字符串
        try {
            // 创建远程url连接对象
            URL url = new URL(httpurl);
            // 通过远程url连接对象打开一个连接，强转成httpURLConnection类
            connection = (HttpURLConnection) url.openConnection();
            // 设置连接方式：get
            connection.setRequestMethod("GET");
            // 设置连接主机服务器的超时时间：15000毫秒
            connection.setConnectTimeout(15000);
            // 设置读取远程返回的数据时间：60000毫秒
            connection.setReadTimeout(60000);
            // 发送请求
            connection.connect();
            // 通过connection连接，获取输入流
            if (connection.getResponseCode() == 200) {
                is = connection.getInputStream();
                // 封装输入流is，并指定字符集
                br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
                // 存放数据
                StringBuffer sbf = new StringBuffer();
                String temp = null;
                while ((temp = br.readLine()) != null) {
                    sbf.append(temp);
                    sbf.append("\r\n");
                }
                result = sbf.toString();
            }
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // 关闭资源
            if (null != br) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if (null != is) {
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            connection.disconnect();// 关闭远程连接
        }

        return result;
    }

//    public static JSONObject doPost(String url, JSONObject json) {
//
//        org.apache.http.client.HttpClient client = HttpClients.createDefault();
//        HttpPost post = new HttpPost(url);
//        //set timeout value
//        RequestConfig requestConfig = RequestConfig.custom()
//                .setConnectTimeout(5000).setConnectionRequestTimeout(1000)
//                .setSocketTimeout(5000).build();
//        post.setConfig(requestConfig);
//        JSONObject response = null;
//        Long start = System.currentTimeMillis();
//        try {
//            StringEntity s = new StringEntity(json.toString());
//            s.setContentEncoding("UTF-8");
//            s.setContentType("application/json");//发送json数据需要设置contentType
//            post.setEntity(s);
//            HttpResponse res = client.execute(post);
//            if (res.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
//                HttpEntity entity = res.getEntity();
//                String result = EntityUtils.toString(res.getEntity());// 返回json格式：
//                System.out.println("----------"+result);
//                //String result = res.getEntity().toString();
//                //response = JSONObject.parseObject(result);
//                response = JSONObject.fromObject(result);
//            }
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//        return response;
//    }
    public static String doPost(String httpUrl, String param) {

        HttpURLConnection connection = null;
        InputStream is = null;
        OutputStream os = null;
        BufferedReader br = null;
        String result = null;
        try {
            URL url = new URL(httpUrl);
            // 通过远程url连接对象打开连接
            connection = (HttpURLConnection) url.openConnection();
            // 设置连接请求方式
            connection.setRequestMethod("POST");
            // 设置连接主机服务器超时时间：15000毫秒
            connection.setConnectTimeout(15000);
            // 设置读取主机服务器返回数据超时时间：60000毫秒
            connection.setReadTimeout(60000);

            // 默认值为：false，当向远程服务器传送数据/写数据时，需要设置为true
            connection.setDoOutput(true);
            // 默认值为：true，当前向远程服务读取数据时，设置为true，该参数可有可无
            connection.setDoInput(true);
            // 设置传入参数的格式:请求参数应该是 name1=value1&name2=value2 的形式。
            connection.setRequestProperty("Content-Type", "application/json;charset=utf-8");
           // connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
            // 设置鉴权信息：Authorization: Bearer da3efcbf-0845-4fe3-8aba-ee040be542c0
           // connection.setRequestProperty("Authorization", "Bearer da3efcbf-0845-4fe3-8aba-ee040be542c0");
            // 通过连接对象获取一个输出流
            os = connection.getOutputStream();
            // 通过输出流对象将参数写出去/传输出去,它是通过字节数组写出的
            os.write(param.getBytes());
            // 通过连接对象获取一个输入流，向远程读取
            if (connection.getResponseCode() == 200) {

                is = connection.getInputStream();
                // 对输入流对象进行包装:charset根据工作项目组的要求来设置
                br = new BufferedReader(new InputStreamReader(is, "UTF-8"));

                StringBuffer sbf = new StringBuffer();
                String temp = null;
                // 循环遍历一行一行读取数据
                while ((temp = br.readLine()) != null) {
                    sbf.append(temp);
                    sbf.append("\r\n");
                }
                result = sbf.toString();
            }
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // 关闭资源
            if (null != br) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (null != os) {
                try {
                    os.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (null != is) {
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            // 断开与远程地址url的连接
            connection.disconnect();
        }
        return result;
    }
}