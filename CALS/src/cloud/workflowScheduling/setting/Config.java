package cloud.workflowScheduling.setting;

import java.io.*;
import java.util.*;

/**
 * Config file for this project
 */
public class Config {
	//because in Java float numbers can not be precisely stored, a very small number E is added before testing whether deadline is met
	public static final double EPS = 0.0000000001;  

	private static boolean isDebug = false;
	private static String workflowLocation;
	private static String outputLocation;
	private static String DOTLocation;
	
	static{	//properties
		try{
	        InputStream is = Workflow.class.getResourceAsStream("config.properties");
	        Properties p = new Properties();
	        p.load(is);
			is.close();
	        String value = (String)(p.get("IS_DEBUG"));
	        Config.isDebug = value.equals("1")?true:false;
	        Config.workflowLocation = (String)(p.get("WORKFLOW_LOCATION"));
	        Config.outputLocation = (String)(p.get("OUTPUT_LOCATION"));
	        Config.DOTLocation = (String)(p.get("dotForWindows"));
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	public static int compareWithEPS(double num1, double num2){
		if(Math.abs(num1-num2)<EPS)
			return 0;
		return Double.compare(num1, num2);
	}
	
	public static boolean isDebug() {
		return isDebug;
	}
	public static String getWorkflowLocation() {
		return workflowLocation;
	}
	public static String getOutputLocation() {
		return outputLocation;
	}
	public static String getDOTLocation() {
		return DOTLocation;
	}
}
