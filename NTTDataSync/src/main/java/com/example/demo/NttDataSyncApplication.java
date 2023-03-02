package com.example.demo;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

import org.apache.beam.runners.direct.DirectRunner;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
@SpringBootApplication
public class NttDataSyncApplication {
	public interface EnricherOptions extends PipelineOptions {
	  
	  }
	public static String instancename = "qpathways-dev:us-central1:qpathwaysdb-dev";
	@Autowired
	public static void main(String[] args) {
		ApplicationContext applicationContext = SpringApplication.run(NttDataSyncApplication.class, args);
		
		
	
		try {
			DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
			options.setRunner(DirectRunner.class);// mandatory//DataflowRunner//DirectRunner
			options.setProject("qpathways-dev");// mandatory
			options.setStreaming(true);// depneds on ur use case
			options.setTempLocation("gs://qpm-demo/temp"); // mandatory 
			options.setStagingLocation("gs://qpm-demo/staging");// mandatory
			options.setRegion("us-central1");// mandatory
//         options.setNumWorkers(4);
			options.setMaxNumWorkers(2);// optional but i would recommend to define it before deploying
			options.setWorkerMachineType("n1-standard-1");// optional but by default it will use n1-standard-1 //larger
//		    options.setUpdate(true);											// - n2-standard-4
			options.setJobName("qpmdocs8-job");// optional
			options.setNetwork("triarqhealth-dev");
			options.setSubnetwork("https://www.googleapis.com/compute/v1/projects/qpathways-dev/regions/us-central1/subnetworks/vm-sql");
//         options.setGcpCredential(GoogleCredentials.fromStream(new FileInputStream("F:\\Working Folder\\Java\\MyProjects\\Dataflow\\pubsub_dev.json")).createScoped(scopes));
//         options.setServiceAccount("qoremigration@qpathways-dev.iam.gserviceaccount.com");

			Pipeline pipeline = Pipeline.create(options);
			// Pull data from PubSub
			

			PCollection<PubsubMessage> pubsubMessage = pipeline.apply("ReadMessages", PubsubIO
					.readMessagesWithAttributes().fromSubscription("projects/qpathways-dev/subscriptions/qpm-publish-sub"));

			//PCollection<KV<String, String>> jdbcdata=pipeline.apply("ReadMessages", JdbcIO.  <KV<String, String>,KV<String, String>>readAll().withDataSourceConfiguration(
	                //JdbcIO.DataSourceConfiguration.create("org.postgresql.Driver","jdbc:postgresql://35.225.26.133:5432/QOREMigrationPractice")));
//	            
//					//.readMessagesWithAttributes().fromSubscription("projects/qpathways-dev/subscriptions/qpm-publish-sub"));

			
			
			
			PCollection<Integer> writeResult;
			writeResult = pubsubMessage.apply(ParDo.of(new PubsubMsgToPostgres()));
			
			
//		
		////System.out.println("writeResult : " + writeResult);
		pipeline.run().waitUntilFinish();
		} catch (Exception e) {
				System.out.println("exception : " + e.getMessage());
			}
	
	}
}
