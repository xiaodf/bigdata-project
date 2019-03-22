package com.bigdata.hive.quanwen;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesUtil {
	//产生一个操作配置文件的对象
	public PropertiesUtil(){
		
	}
	public void Properties() {
		try {
			InputStream inputStream=this.getClass().getClassLoader().getResourceAsStream(System.getProperty("user.dir")+"/config.properties");
			Properties prop=new Properties();
			prop.load(inputStream);
			inputStream.close();
		} catch (FileNotFoundException ex) {
			// TODO Auto-generated catch block
			 System.out.println("读取属性文件--->失败！- 原因：文件路径错误或者文件不存在");
			 ex.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("装载文件--->失败!");
			e.printStackTrace();
		}
	}
}
