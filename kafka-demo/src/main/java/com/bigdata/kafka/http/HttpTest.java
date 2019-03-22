package com.bigdata.kafka.http;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Created by x on 10/9/16.
 */
public class HttpTest {
	public static void main(String args[]) throws IOException {
		StringBuilder result = new StringBuilder();
		URL url = new URL("http://www.baidu.com");
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setRequestMethod("GET");
		BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
		String line;
		while ((line = rd.readLine()) != null) {
			result.append(line);
		}
		rd.close();
		System.out.println(result.toString());
	}
}
