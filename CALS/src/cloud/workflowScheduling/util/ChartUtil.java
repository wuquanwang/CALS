package cloud.workflowScheduling.util;

import java.awt.*;
import java.io.*;
import java.util.*;
import java.util.List;

import javax.swing.*;

import org.jfree.chart.*;
import org.jfree.chart.annotations.*;
import org.jfree.chart.axis.*;
import org.jfree.chart.labels.*;
import org.jfree.chart.plot.*;
import org.jfree.chart.renderer.xy.*;
import org.jfree.data.xy.*;
import org.jfree.ui.*;

import cloud.workflowScheduling.contentionAware.*;
import cloud.workflowScheduling.contentionFree.*;
import cloud.workflowScheduling.setting.*;

/**
 * Using the JFreeChart package to draw charts
 * @author wu
 */
public class ChartUtil {
	private static final char SZERO = 0x2080; // subscript zero in Unicode
	private static final float ANNOT_FONT_SIZE = 20;
	private static final Font FONT = new Font("Helvetica", Font.PLAIN, 14);
	private static final double THEIGHT = 0.12; //所以任务的高度是0.24
	private static final double EHEIGHT = 0.18; //边的高度就是0.18
	private static ChartPanel panel;
	private static boolean showAnnotFlag = false;
	private static boolean exportPNGFlag = false;
	/**
	 * visualize a schedule solution @param s
	 * dynamic update is supported	
	 * 注意：不同于Solution，CSolution在visualize时，可能一条send边，对应多条receive边的
	 */
	public static void visualizeSchedule(Solution s) {
	    XYPlot plot = new XYPlot();
		
		//XYIntervalSeries相当于对应了一个矩形，x和y轴是相反的
		XYIntervalSeriesCollection dataset = new XYIntervalSeriesCollection();
		XYIntervalSeries[] series = new XYIntervalSeries[3];
	    String[] names = {"execute", "send", "receive"};
	    for(int i = 0;i<names.length;i++){
		    series[i] = new XYIntervalSeries(names[i]);
		    dataset.addSeries(series[i]);
	    }
		
		//vm
	    String[] vms = new String[s.getUsedVMSet().size()];
	    for(VM vm : s.getUsedVMSet()){
	    	int id = vm.getId();
	    	int type = vm.getType();
	    	vms[id] = "vm" + id+"_"+type;
//	    	vms[id] = "v" + (char)(id + 1 + SZERO);	//仅用于画示例的
	    }

	    // task
	    for(Task task : s.getAllocatedTaskSet()){
	    	for(TAllocation alloc : s.getTAList(task)){
	    		if(alloc.getTask().getTaskSize() == 0)
	    			continue;
	    		
		    	int vmid =  alloc.getVM().getId();
		    	double startTime = alloc.getStartTime();
		    	double finishTime = alloc.getFinishTime();
		        series[0].add(vmid, vmid - THEIGHT, vmid + THEIGHT,
		        		  startTime, startTime, finishTime);
		        
		        if(showAnnotFlag)
			        try{
			        	String tName = alloc.getTask().getName();
			        	char c = (char)(SZERO+Integer.parseInt(tName));
				        XYTextAnnotation annot1 = new XYTextAnnotation("n"+c,
				        		vmid, (int)(startTime+ (finishTime-startTime)*0.5));
				        annot1.setFont(annot1.getFont().deriveFont(ANNOT_FONT_SIZE));
				        plot.addAnnotation(annot1);
			        }catch(Exception e){}
	    	}
	    }
	    
	    if(s instanceof CSolution){
	    	//先画receive 
	    	forEdges((CSolution)s, true, series, plot);
	    	
	    	//再画send
	    	forEdges((CSolution)s, false, series, plot);
	    }

	    XYBarRenderer renderer = new XYBarRenderer();
	    renderer.setUseYInterval(true);
	    renderer.setShadowVisible(false);
	    renderer.setBaseToolTipGenerator(new StandardXYToolTipGenerator());
	    renderer.setDrawBarOutline(true);
	    renderer.setBarPainter(new StandardXYBarPainter());//默认的为GradientXYBarPainter，有点渐变
	    //设置label
//	    NumberFormat format = NumberFormat.getNumberInstance();
//	    format.setMaximumFractionDigits(2); // etc.
//	    renderer.setBaseItemLabelGenerator(new StandardXYItemLabelGenerator("{0} {1} {2}", format, format));
//	    renderer.setBaseItemLabelsVisible(true);
	    
//	    XYPlot plot = new XYPlot(dataset, new SymbolAxis("VMs", vms), new NumberAxis("time"), renderer);
	    plot.setDataset(dataset);
	    plot.setRangeAxis(new NumberAxis("time"));
	    plot.setDomainAxis(new SymbolAxis("", vms));

	    plot.getRangeAxis().setLabelFont(FONT);
//	    plot.getRangeAxis().setRange(0, 70);		//仅用于画示例的
	    plot.getRangeAxis().setTickLabelFont(FONT); 
	    plot.getDomainAxis().setLabelFont(FONT);
	    plot.getDomainAxis().setTickLabelFont(FONT);
	    renderer.setBaseLegendTextFont(FONT);
	    plot.setRenderer(renderer);
	    plot.setOrientation(PlotOrientation.HORIZONTAL);
	    JFreeChart chart = new JFreeChart(plot);

	    if(exportPNGFlag)
		    try{
			    int scale =3 ; //chart will be rendered at thrice the resolution.
		    	OutputStream os = new FileOutputStream("D:\\aa"+s.getMakespan()+".png");
		    	ChartUtilities.writeScaledChartAsPNG(os, chart, 800, 500, scale, scale);
		    	os.close();
		    }catch(IOException e){}	    
	    if(panel==null){
		    panel = new ChartPanel(chart);
		     
		    JFrame jframe = new JFrame("Visualize a schedule");
		    jframe.setContentPane(panel);
		    jframe.pack();
		    RefineryUtilities.centerFrameOnScreen(jframe);
		    jframe.setVisible(true);
	    }else{
	    	panel.setChart(chart);
	    	panel.repaint();
	    }
	}
	private static void forEdges(CSolution s, boolean isIn, XYIntervalSeries[] series, XYPlot plot) {
		
		for(VM vm : s.getUsedVMSet()){
			List<EAllocation> l = isIn ? s.getEAInList(vm) : s.getEAOutList(vm);
			if(l == null || l.size()==0) //该vm上没有边传输
				continue;
			
			List<EAllocation> list = new ArrayList<EAllocation>(l);
			Collections.sort(list, (EAllocation a, EAllocation b)->{
				return Double.compare(a.getStartTime(), b.getStartTime());
			});
			for(int i = 0;i<list.size();i++){
				EAllocation ea = list.get(i);
				
				boolean conflictFlag = false;
				if(i>0){
					EAllocation lastEA = list.get(i-1);
					if(lastEA.getFinishTime()>ea.getStartTime()+Config.EPS)
						conflictFlag = true;
				}
				double conflict = conflictFlag ? EHEIGHT:0;
				
				double startTime = ea.getStartTime();
		    	double finishTime = ea.getFinishTime();
		    	if(startTime != finishTime){	//传输大小为0的边不画出来
		        	String tName1 = ea.getEdge().getSource().getName();
		        	String tName2 = ea.getEdge().getDestination().getName();
		    		
		        	XYTextAnnotation annot = null;
		    		if(isIn){
				    	int dvmid = ea.getDestVM().getId();
				    	series[2].add(dvmid, dvmid + THEIGHT+conflict, dvmid + THEIGHT+ EHEIGHT+conflict,
			        		  startTime, startTime , finishTime);
				    	if(showAnnotFlag){
				        	char c1 = (char)(SZERO + Integer.parseInt(tName1));
				        	char c2 = (char)(SZERO + Integer.parseInt(tName2));
					    	annot = new XYTextAnnotation("e"+c1+","+c2,dvmid+THEIGHT+EHEIGHT/2+conflict, 
					        		(int)(startTime+ (finishTime-startTime)*0.5));
				    	}
		    		}else{
				    	int svmid = ea.getSourceVM().getId();
				    	series[1].add(svmid, svmid - THEIGHT-conflict, svmid - THEIGHT- EHEIGHT-conflict,
			        		  startTime, startTime , finishTime);
				    	if(showAnnotFlag){
				        	char c1 = (char)(SZERO + Integer.parseInt(tName1));
				        	char c2 = (char)(SZERO + Integer.parseInt(tName2));
					    	annot = new XYTextAnnotation("e"+c1+","+c2,svmid-THEIGHT-EHEIGHT/2-conflict,
					        		(int)(startTime+ (finishTime-startTime)*0.5));
				    	}
		    		}
		    		if(showAnnotFlag){
				        annot.setFont(annot.getFont().deriveFont(ANNOT_FONT_SIZE));
				        plot.addAnnotation(annot);
		    		}
		    	}
			}
		}
	}
	/**
	 * visualize a schedule solution @param s with a new window
	 */
	public static void visualizeScheduleNew(Solution s){
		panel = null;
		visualizeSchedule(s);
	}
	
	/**
	 * visualize a Pareto front for @param data
	 */
	public static void visualizePareto2D(double[][] data){
		List<double[]> l = new ArrayList<double[]>();
		for(double[] d : data)
			l.add(d);

		List<List<double[]>> list = new ArrayList<List<double[]>>();
		list.add(l);
		List<String> names = new ArrayList<String>();
		names.add("test");
		visualizePareto2D(list,names);
	}
	/**
	 * visualize a Pareto front for @param data with @param names
	 */
	public static void visualizePareto2D(List<List<double[]>> data, List<String> names) {
		XYSeriesCollection dataset = new XYSeriesCollection();

		int i = 0;
		for(List<double[]> list : data){
			XYSeries series = new XYSeries(names.get(i) + (i++)); //为了避免重名异常
			for (double a[]: list) 
				series.add(a[0], a[1]);
			dataset.addSeries(series);
		}

		JFreeChart chart = ChartFactory.createScatterPlot("Non-dominated Points", "f1", "f2", dataset);
		ChartPanel panel = new ChartPanel(chart);

		JFrame jFrame = new JFrame("Visualize a list");
		jFrame.setContentPane(panel);
		jFrame.setSize(600, 500);
		jFrame.setLocationRelativeTo(null);
//		jFrame.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
		jFrame.setVisible(true);
	}
	

	private static XYSeries series2D = new XYSeries("PF"); 
	private static XYSeriesCollection dataset2D;
	private static ChartPanel panel2D;
	/**
	 * visualize a Pareto front for @param data with @param tip
	 * dynamic update is supported
	 */
	public static void visualizePareto2D(List<CSolution> data, String tip) {
		series2D.clear();
		series2D.setKey(tip);
		for(CSolution c : data){
			series2D.add(c.getMakespan(), c.getCost());
		}

		if(dataset2D == null){
			dataset2D = new XYSeriesCollection();
			dataset2D.addSeries(series2D);
			
			JFreeChart chart = ChartFactory.createScatterPlot("Non-dominated Points", "f1", "f2", dataset2D);
			panel2D = new ChartPanel(chart);
	
			JFrame jFrame = new JFrame("Visualize a list");
			jFrame.setContentPane(panel2D);
			jFrame.setSize(600, 500);
			jFrame.setLocationRelativeTo(null);
			jFrame.setVisible(true);
		}else{
			panel2D.repaint();
		}
	}
}