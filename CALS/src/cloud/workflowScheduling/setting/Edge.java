package cloud.workflowScheduling.setting;

/**
 * an edge in the workflow
 */
public class Edge {

	private Task source;
	private Task destination;
	private long dataSize;    

	/**
	 * constructs an edge with two tasks: @param source, @param destination
	 */
	public Edge(Task source, Task destination) {
		this.source = source;
		this.destination = destination;
	}
	
	//-------------------------------------getters && setters--------------------------------
	public long getDataSize() {
		return dataSize;
	}
	public double getTransferTime(){
		return dataSize / VM.NETWORK_SPEED;
	}
	public Task getSource() {
		return source;
	}
	public Task getDestination() {
		return destination;
	}
	//only used by WorkflowParser class
	void setDataSize(long size) {
		this.dataSize = size;
	}

	//-------------------------------------overrides--------------------------------
	public String toString() {
		String s = "Edge [" + source.getName() + " -> " + destination.getName()
		 	+ ", size=" + dataSize + "]";
		return s;
	}
}