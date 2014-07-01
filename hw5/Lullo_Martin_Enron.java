import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;
import java.io.BufferedReader;
import java.io.*;
import java.io.File;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Lullo_Martin_Enron {
  public static void main(String[] args) throws IOException {
 
	String line;
	String body_line;
    Configuration config = HBaseConfiguration.create();
    HTable table = new HTable(config, "mlenron1");
    HTable table2 = new HTable(config, "mlenron2");

	  File dir = new File("/home/public/course/enron_mail_20110402/maildir");

	  File [] mainfolder = dir.listFiles();
	  for (int y = 0; y <10; y++){ //run for 10 users
	
	  File userpath = new File(mainfolder[y].getAbsolutePath());
	  File [] userfolder = userpath.listFiles();
	  
		  for (int x = 0; x <userfolder.length; x++){	//loops through each user folder
		  File folderpath = new File(userfolder[x].getAbsolutePath());
		  File[] directoryListing = folderpath.listFiles();
		  
			  if (directoryListing != null) {
			    for (int i = 0; i < directoryListing.length; i++) {
			    	BufferedReader fileIn = new BufferedReader(new FileReader(directoryListing[i].getAbsoluteFile()));
			    	String from = "";
			    	String email_from = "";
			    	String month = "";
			    	String date = "";
			    	String send_to = "";
			    	String email_body = "";
			    	while((line = fileIn.readLine()) != null){
					    	
					    	if (line.contains("From: ")){
					    		if (!line.contains("X-From: "))
					    			email_from = line.replace("From: ", "");
					    	}
					    	if (line.contains("Date: ")){
					    		String [] tokens = line.split(" ");
					    		month = tokens[3];
					    		date = line.replace("Date: ", "");
					     	}
					    	if (line.contains("To: ")){
					    		if(!line.contains("X-To: "))	
					    			send_to=line.replace("To: ", "");
					    	}
					    	if (line.contains("FileName: ")){
					    		fileIn.readLine(); //skip line
					    		while((body_line = fileIn.readLine()) != null){
					    			email_body = email_body + " " + body_line;
					    		}fileIn.close();
					    		break;
				    	}
			    	}
			    	{	try		{Put p = new Put(Bytes.toBytes(mainfolder[y].getName() + "_"+ month + "_"+"Message"+i));
				    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("date"),Bytes.toBytes(date));
				    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("email_from"),Bytes.toBytes(email_from));
				    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("send_to"),Bytes.toBytes(send_to));
				    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("email_body"),Bytes.toBytes(email_body));
				    table.put(p);} 
			    		finally {}
			    	}

			    	{	try		{Put p2 = new Put(Bytes.toBytes(month + "_"+ mainfolder[y].getName() + "_"+"Message"+i));
				    p2.add(Bytes.toBytes("cf1"), Bytes.toBytes("date"),Bytes.toBytes(date));
				    p2.add(Bytes.toBytes("cf1"), Bytes.toBytes("email_from"),Bytes.toBytes(email_from));
				    p2.add(Bytes.toBytes("cf1"), Bytes.toBytes("send_to"),Bytes.toBytes(send_to));
				    p2.add(Bytes.toBytes("cf1"), Bytes.toBytes("email_body"),Bytes.toBytes(email_body));
				    table2.put(p2);} 
			    		finally {}
			    	}
			    	   	
			    	
			    	
			    }
			  
			  }
		  }
	  }
	 } 
  }