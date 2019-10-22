package cloud.workflowScheduling.contentionAware;

import java.io.*;
import java.util.*;

import cloud.workflowScheduling.contentionFree.*;
import cloud.workflowScheduling.setting.*;
import cloud.workflowScheduling.util.*;

public class Evaluation {
	public static void main(String[] args)throws Exception{
//		testSynthetic();
		
		//D:/test.dot
		//D:\\test1.dot					结果：	482  支持复制则为411
		//F:\\dax\\floodplain.xml				23400
		//F:\\dax\\SIPHT\\SIPHT.n.100.1.dax		1019
		//F:\\dax\\CYBERSHAKE\\CYBERSHAKE.n.100.1.dax		151 	支持复制则为51
		//CYBERSHAKE, MONTAGE, LIGO, GENOME, SIPHT
		test("F:\\dax\\CYBERSHAKE\\CYBERSHAKE.n.50.1.dax", true);
	}
	
	private static void testSynthetic()throws Exception{
		BufferedWriter br = new BufferedWriter(new FileWriter("F:\\result.txt"));
//		testSyntheticStep(40, 1, 0.5, 1, 0.5, br);	//test
		
    	double[] CCR = { 0.1, 0.2, 0.5, 1, 2, 5};	//0.05
		int[] numbers = {20, 40, 60, 80, 100};
		for(int n = 0;n<numbers.length;n++){
			for(int i = 0; i<CCR.length;i++){
				for(double density = 0.3; density<=0.7;density+=0.2){
					for(double alpha = 0.5;alpha<=2;alpha*=2){
						for(double regular = 0.2;regular<=0.8;regular+=0.3){
							testSyntheticStep(numbers[n], CCR[i], density,alpha,regular, br);
						}
					}
				}
			}
		}
	}
	
	private static void testSyntheticStep(int n, double CCR, double density,
			double alpha, double regular, BufferedWriter br)throws Exception{
		RandomGraphGenerator g = new RandomGraphGenerator();
		String arguments = "-d -a ";
		arguments += "-n"+n+" ";
		int l = (int)(Math.sqrt(n)*alpha);
		int L = (int)(l*regular);
		arguments += "-l"+l+" -L"+L+" ";
		arguments += "-c" + density + " ";
		arguments += "-v100 -V50 ";
		int e = (int)(100*CCR);
		arguments += "-e" + e + " -E"+(e/2);
		String s = g.generateGraphFile("D:/test.dot",arguments);
//		GraphViz.writeDOTGraphToImageFile(s, "png", "D:/test.png");
		
    	String result = test("D:/test.dot", false);
    	br.write(n + "\t" + CCR + "\t"+ density + "\t"
    			 + alpha + "\t"+ regular + "\t"	
    			 +result + "\r\n");
    	br.flush();
	}
	
	private static String test(String file, boolean visualizeFlag){
    	Workflow wf = new Workflow(file);	
    	List<CSolution> list = new ArrayList<CSolution>();
    	
    	LSUtil lsUtil = new LSUtil(wf);
    	Solution solution = lsUtil.listScheduling(wf, TProperties.Type.B_LEVEL, 1);
    	
    	Adaptor adaptor = new Adaptor();
    	list.add(adaptor.convertToIdealCSolution(solution));
    	list.add(adaptor.buildFromSolutionShared(solution, wf));
    	list.add(adaptor.buildFromSolutionExclusive(solution, wf, TProperties.Type.B_LEVEL));

    	CALS cals = new CALS();
    	long t1 = System.currentTimeMillis();
    	list.add(cals.listSchedule(wf, TProperties.Type.GAMMA, 0));
    	long t2 = System.currentTimeMillis();
    	list.add(cals.listSchedule(wf, TProperties.Type.GAMMA, 1));
    	long t3 = System.currentTimeMillis();
//    	list.add(cals.listSchedule(wf, TProperties.Type.GAMMA,2));
//    	list.add(cals.dynamicLS(wf));
    	
    	String result ="";
    	for(CSolution c : list){
    		result += c.getMakespan()+"\t";
	    	System.out.println(c.getMakespan()+"\t"+c.getCost() + "\t"+ c.validate(wf));
	    	if(visualizeFlag)
    			ChartUtil.visualizeScheduleNew(c);
		}
    	System.out.println("runtime:" + (t2-t1)+"\t"+(t3-t1));
    	
    	//used for data collection
    	result += wf.getSequentialLength() +"\t" + wf.getCPTaskLength();
    	System.out.println(result);
    	return result;
	}
}
