package com.bigdata.sqoop2;

import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.MConnection;
import org.apache.sqoop.model.MConnectionForms;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MJobForms;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.submission.counter.Counter;
import org.apache.sqoop.submission.counter.CounterGroup;
import org.apache.sqoop.submission.counter.Counters;
import org.apache.sqoop.validation.Status;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

public class Sqoop2Oracle {

	public void sqoop(String ip) {
		System.out.println("进行sqoop操作----");
		String sqoopurl = "http://10.199.33.12:12000/sqoop/";
		SqoopClient client = new SqoopClient(sqoopurl);
		MConnection newCon = client.newConnection(1);

		// Get connection and framework forms. Set name for connection
		System.out.println("Get connection and framework forms. Set name for connection");
		MConnectionForms conForms = newCon.getConnectorPart();
		MConnectionForms frameworkForms = newCon.getFrameworkPart();
		newCon.setName("MyConnection");

		// Set connection forms values
		System.out.println("Set connection forms values");
		//conForms.getStringInput("connection.connectionString").setValue("jdbc:oracle:thin:@10.10.14.152:1521:orcl");
		//conForms.getStringInput("connection.jdbcDriver").setValue("oracle.jdbc.driver.OracleDriver");// "com.mysql.jdbc.Driver");
		conForms.getStringInput("connection.connectionString").setValue("jdbc:mysql://"+ip+":3306/test");
		conForms.getStringInput("connection.jdbcDriver").setValue("com.mysql.jdbc.Driver");// "com.mysql.jdbc.Driver");
		conForms.getStringInput("connection.username").setValue("root");// "root");
		conForms.getStringInput("connection.password").setValue("111111");
		frameworkForms.getIntegerInput("security.maxConnections").setValue(0);

		//check connection status
		Status status = client.createConnection(newCon);
		if (status.canProceed()) {
			System.out.println("Created. New Connection ID : " + newCon.getPersistenceId());
		} else {
			System.out.println("Check for status and forms error ");
		}

		MJob newjob = client.newJob(newCon.getPersistenceId(), org.apache.sqoop.model.MJob.Type.IMPORT);
		MJobForms connectorForm = newjob.getConnectorPart();
		MJobForms frameworkForm = newjob.getFrameworkPart();

		System.out.println("create job");
		newjob.setName("importJob");
		// Database configuration
		// connectorForm.getStringInput("table.schemaName").setValue("");
		// Input either table name or sql
		connectorForm.getStringInput("table.tableName").setValue("tb_blob2");
		// connectorForm.getStringInput("table.sql").setValue("select * from
		// (select p.* from person p left outer join address a on p.id =
		// a.person_id)a where ${CONDITIONS}");
		// connectorForm.getStringInput("table.columns").setValue("id,name");
		connectorForm.getStringInput("table.partitionColumn").setValue("name");
		// Set boundary value only if required
		// connectorForm.getStringInput("table.boundaryQuery").setValue("");

		// Output configurations
		frameworkForm.getEnumInput("output.storageType").setValue("HDFS");
		frameworkForm.getEnumInput("output.outputFormat").setValue("TEXT_FILE");// Other
																				// option:
																				// SEQUENCE_FILE
		// System.out.println(date);
		frameworkForm.getStringInput("output.outputDirectory").setValue("/user/outputHive/oracletohive2");

		// Job resources
		frameworkForm.getIntegerInput("throttling.extractors").setValue(1);
		frameworkForm.getIntegerInput("throttling.loaders").setValue(1);

		Status statuss = client.createJob(newjob);
		if (statuss.canProceed()) {
			System.out.println("New Job ID: " + newjob.getPersistenceId());
		} else {
			System.out.println("Check for status and forms error  from JOB ");
		}

		// Print errors or warnings
		System.out.println(newjob.getConnectorPart().getForms());
		System.out.println(newjob.getFrameworkPart().getForms());

		// Job submission start
		MSubmission submission = client.startSubmission(newjob.getPersistenceId());
		System.out.println("Status : " + submission.getStatus());
		if (submission.getStatus().isRunning() && submission.getProgress() != -1) {
			System.out.println("Progress : " + String.format("%.2f %%", submission.getProgress() * 100));
		}
		System.out.println("Hadoop job id :" + submission.getExternalId());
		System.out.println("Job link : " + submission.getExternalLink());
		Counters counters = submission.getCounters();
		if (counters != null) {
			System.out.println("Counters:");
			for (CounterGroup group : counters) {
				System.out.print("\t");
				System.out.println(group.getName());
				for (Counter counter : group) {
					System.out.print("\t\t");
					System.out.print(counter.getName());
					System.out.print(": ");
					System.out.println(counter.getValue());
				}
			}
		}
		if (submission.getExceptionInfo() != null) {
			System.out.println("Exception info : " + submission.getExceptionInfo());
		}

		// Check job status
		if (submission.getStatus().isRunning() && submission.getProgress() != -1) {
			System.out.println("Progress : " + String.format("%.2f %%", submission.getProgress() * 100));
		}
	}

	public static void main(String[] args) {
		String ip = "10.199.33.13";
		Sqoop2Oracle so = new Sqoop2Oracle();
		so.sqoop(ip);
	}
}
