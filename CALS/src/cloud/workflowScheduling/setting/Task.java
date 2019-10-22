package cloud.workflowScheduling.setting;

import java.util.*;

/**
 * a task in the workflow, including edge information
 */
public class Task {
	public static enum TEdges{IN, OUT}
	
	private String name;
	private double taskSize;

	//adjacent list to store edge information
	//由于子边的终端之间可能也存在父子关系，所以这些edge都是按照其终端对应的拓扑顺序进行排序的;通过workflow中的topoSort函数实现
	private List<Edge> outEdges = new ArrayList<Edge>();	
	private List<Edge> inEdges = new ArrayList<Edge>();

	public Task(String name, double taskSize) {
		this.name = name;
		this.taskSize = taskSize;
	}

	// The following two methods are only used during constructing a workflow
	void insertEdge(TEdges inOrOut, Edge e){
		if(inOrOut == TEdges.IN){
			if(e.getDestination()!=this)
				throw new RuntimeException();	
			inEdges.add(e);
		}else{
			if(e.getSource()!=this)
				throw new RuntimeException();
			outEdges.add(e);
		}
	}
	void sortEdges(TEdges inOrOut, Comparator<Edge> comp){
		if(inOrOut == TEdges.IN)
			Collections.sort(inEdges, comp);
		else
			Collections.sort(outEdges, comp);
	}
	//-------------------------------------getters&setters--------------------------------
	public String getName() {
		return name;
	}
	public double getTaskSize() {
		return taskSize;
	}
	public List<Edge> getOutEdges() {
		return outEdges==null ? null : Collections.unmodifiableList(outEdges);
	}
	public List<Edge> getInEdges() {
		return inEdges==null ? null : Collections.unmodifiableList(inEdges);
	}
	
	//-------------------------------------overrides--------------------------------
	public String toString() {
		return "Task [" + name + ", size=" + taskSize +"]";
	}
}