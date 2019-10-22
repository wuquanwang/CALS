package cloud.workflowScheduling.setting;

/**
 * virtual machine, i.e., cloud service resource
 */
public class VM {
	public static final double LAUNCH_TIME = 0;	
	public static final double NETWORK_SPEED = 20 * 1000*1000; // 20 MB
	
	public static final int TYPE_NO = 9;
	public static final double[] SPEEDS = {1, 1.5, 2, 2.5, 3, 3.5, 4, 4.5, 5};	//L-ACOÖÐÓÃµÄ
//	public static final double[] SPEEDS = {0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5};	
	public static final double[] UNIT_COSTS = {0.12, 0.195, 0.28, 0.375, 0.48, 0.595, 0.72, 0.855, 1};
	public static final double INTERVAL = 3600;	//one hour, billing interval

	public static final int FASTEST = 8;
	public static final int SLOWEST = 0;
	
	private int id;
	private int type; 

	//only invoked by Solution, as id only make senses in one Solution
	public VM(int id, int type){
		this.id = id;
		this.type = type;
	}
	
	//------------------------getters && setters---------------------------
	public double getSpeed(){		return SPEEDS[type];	}
	public double getUnitCost(){		return UNIT_COSTS[type];	}
	public int getId() {		return id;	}		
	public int getType() {		return type;	}
	public void setType(int type) {			this.type = type;	}
	
	//-------------------------------------overrides--------------------------------
	public String toString() {
		return "VM [" + id + ", type=" + type + "]";
	}
}