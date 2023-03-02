package com.example.demo;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.DateTime;
import org.json.JSONObject;

import com.google.gson.Gson;

public class PubsubMsgToPostgres extends DoFn<PubsubMessage, Integer>{
private static final long serialVersionUID = 1L;
	

	@ProcessElement
	public void processElement(ProcessContext c)
	{
		//System.out.println("2");
		Integer rows = 0;
        PubsubMessage msg = null;
        DateTime publishtime;
        org.json.JSONArray jsonPaylod = null;
        //MsgProcessor msgProc=null;
       // int processElement = 0;
       Integer cnt=null;
        try
	    {
        	
        	msg = c.element();
        	
            publishtime =  c.timestamp().toDateTime();
            String payload = new String(msg.getPayload());
            Gson g = new Gson(); 
            String str = g.toJson(payload); 
           
            System.out.println("----------------------------------------------------------------");
             
            System.out.println(payload);
           
        	
//            JSONArray  jsonArray = new JSONArray();
//            jsonArray.add(payload);
           
            //String str1 = "[{\"No\":\"17\",\"Name\":\"Andrew\"},{\"No\":\"18\",\"Name\":\"Peter\"}, {\"No\":\"19\",\"Name\":\"Tom\"}]";
           // String str2=str.replace("'\"'", "");
            // jsonPaylod = new org.json.JSONArray(payload);  
            

//            for(int i=0;i<jsonArray.length();i++)
//            {
//                JSONObject jsonObject1 = jsonArray.getJSONObject(i);
//                String value1 = jsonObject1.optString("key1");
//                String value2 = jsonObject1.optString("key2");
//                String value3 = jsonObject1.optString("key3");
//                String value4 = jsonObject1.optString("key4");
//            }
//            
//            
//            
//            for (Object o : jsonArray) {
//                JSONObject jsonLineItem = (JSONObject) o.;
//                String key = jsonLineItem.getString("key");
//                String value = jsonLineItem.getString("value");
//                ...
//            }
//
//                       jsonPaylod = new JSONObject(payload);

//        	msgProc = new MsgProcessor(Long.parseLong(msg.getAttribute("MessageID")),jsonPaylod,DemoApplication.username,DemoApplication.password, DemoApplication.qoreurl,DemoApplication.instancename);
//        	System.out.println(jsonPaylod);
//        	rows = msgProc.processor(true, null);
//        	rows++;
//        	System.out.println(rows++);
	    }catch(Exception e){
	    	System.out.println(e.toString());
	    }
        finally
        {
        	msg = null;
        	jsonPaylod = null;
        	publishtime = null;
        	//msgProc = null;
        }
        c.output(rows);
     
    }

}
