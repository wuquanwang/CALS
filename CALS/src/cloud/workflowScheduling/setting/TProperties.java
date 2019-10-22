package cloud.workflowScheduling.setting;

import static java.lang.Math.*;
import java.util.*;

// Task properties: heuristic information of tasks, e.g., bLvel, tLevel, gamma, Probabilistic Upward Rank  
@SuppressWarnings("serial")
public class TProperties extends HashMap<Task, Double> implements Comparator<Task>{
	//TProperties Type
	public static enum Type{B_LEVEL, T_LEVEL, S_LEVEL, PU_RANK, GAMMA}
	Workflow wf;

	public TProperties(Workflow wf, TProperties.Type type){
		super();
		this.wf = wf;

		double speed = VM.SPEEDS[VM.FASTEST];	// VM.SPEEDS[4];
		if(type == TProperties.Type.B_LEVEL){
			for(int j= wf.size()-1; j>=0; j--){
				double bLevel = 0;	
				Task task = wf.get(j);
				for(Edge outEdge : task.getOutEdges()){
					Double childBLevel = this.get(outEdge.getDestination());
					bLevel = Math.max(bLevel, childBLevel + outEdge.getDataSize() / VM.NETWORK_SPEED);
				}
				bLevel += task.getTaskSize() / speed;
				this.put(task, bLevel);
			}
		}else if(type == TProperties.Type.S_LEVEL){
			for(int j= wf.size()-1; j>=0; j--){
				double sLevel = 0;	
				Task task = wf.get(j);
				for(Edge outEdge : task.getOutEdges()){
					Double childSLevel = this.get(outEdge.getDestination());
					sLevel = Math.max(sLevel, childSLevel);
				}
				sLevel += task.getTaskSize() / speed;
				this.put(task, sLevel);
			}
		}else if(type == TProperties.Type.T_LEVEL){	//T_LEVEL目前还没有使用的
			for(Task task : wf.getTaskList()){
				double arrivalTime = 0;
				for(Edge inEdge : task.getInEdges()){
					Task parent = inEdge.getSource();
					Double parentTLevel = this.get(parent);
					arrivalTime = Math.max(arrivalTime, parentTLevel + 
							parent.getTaskSize() / speed + inEdge.getDataSize() / VM.NETWORK_SPEED);
				}
				this.put(task, arrivalTime);
			}
		}else if(type == TProperties.Type.GAMMA){
			for(int j= wf.size()-1; j>=0; j--){
				Task task = wf.get(j);
				double gamma = 0;
				for(Edge outEdge : task.getOutEdges()){
					Double childGamma = this.get(outEdge.getDestination());
					gamma = Math.max(gamma, childGamma);
				}
				gamma += task.getTaskSize() / speed;
				for(Edge inEdge : task.getInEdges()){
					gamma += inEdge.getDataSize() / VM.NETWORK_SPEED;
				}
				this.put(task, gamma);
			}
		}
	}	

	/**
	 * calculate the 'probabilistic upward rank' for each task based on @param theta
	 * it may be different each time it is calculated. Called by ProLiS, LACO and BLACO.
	 */
	public TProperties(Workflow wf, TProperties.Type type, double theta){
		if(type != TProperties.Type.PU_RANK)
			throw new RuntimeException();
		this.wf = wf;
		
		double speed = VM.SPEEDS[VM.FASTEST];
		for(int j= wf.size()-1; j>=0; j--){
			double pURank = 0;	
			Task task = wf.get(j);
			for(Edge outEdge : task.getOutEdges()){
				Task child = outEdge.getDestination();
				
				int flag = 1;
				if(theta != Double.MAX_VALUE){		// if theta = Double.MAX_VALUE, flag = 1
					double et = child.getTaskSize() / speed;
					double tt = outEdge.getDataSize() / VM.NETWORK_SPEED;
					double d = 1-Math.pow(theta, -et / tt);	//网络传输时间越大，d取值越接近于1
					if(d<random())
						flag = 0;
				}
				
				Double childPURank = this.get(child);
				pURank = Math.max(pURank, childPURank + flag * outEdge.getDataSize() / VM.NETWORK_SPEED);
			}
			this.put(task,pURank + task.getTaskSize() / speed);
		}
	}
	
	//注：实际上在比较的时候t_level并没有用过，都是用的b_level，或者其改进版
	public int compare(Task o1, Task o2) {
		// to keep entry node ranking last, and exit node first
		if(o1.getName().equals("entry") || o2.getName().equals("exit"))	
			return 1;
		if(o1.getName().equals("exit") || o2.getName().equals("entry"))	
			return -1;
		
		double value1 = this.get(o1);
		double value2 = this.get(o2);
		if(value1 > value2)
			return 1;
		else if(value1 < value2)
			return -1;
		else{	//避免直接相等导致在TreeSet等数据结构中就产生了覆盖；此处并不会影响排序结果的(已测试过)
			return wf.indexOf(o1) - wf.indexOf(o2);	
		}
	}
}