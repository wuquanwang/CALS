package cloud.workflowScheduling.contentionFree;

import cloud.workflowScheduling.setting.*;

/**
 * Task Allocation Information
 * @author wu
 */
public class TAllocation extends Allocation {

	private Task task;
	private VM vm;
	
	protected TAllocation(){}
	public TAllocation(VM vm, Task task, double startTime) {
		this.vm = vm;
		this.task = task;
		this.startTime = startTime;
		if(vm != null && task !=null)
			this.finishTime = startTime + task.getTaskSize() / vm.getSpeed();
	}

	//-------------------------------------getters & setters--------------------------------
	public VM getVM(){
		return vm;
	}
	public Task getTask() {
		return task;
	}
	
	//-------------------------------------overrides--------------------------------
	// 这里不加入VM信息，只是为了Solution在toString时更清晰
	public String toString() {
		return "Allocation [task=" + task.getName() + ": "+ startTime
				+ ", " + finishTime + "]";
	}

	@Override
    public boolean equals(Object obj) {
		TAllocation a2=(TAllocation)obj;
		if(a2 == null)
			return false;
		
		boolean flag = task == a2.task;
		flag &= (vm.getType() == a2.vm.getType())
				&&vm.getId() == a2.vm.getId();
		flag &= startTime == a2.startTime;
		if(flag)
			return true;
		return false;
    }
	//定义了equals方法后，hashcode方法的定义成了必须
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((task == null) ? 0 : task.hashCode());
		result = prime * result + ((vm == null) ? 0 : vm.getType());
		result = prime * result + ((vm == null) ? 0 : vm.getId());
		result = result + (int)(prime * startTime);
		return result;
	}

	//-------------------------------------only for ICPCP---------------------------
    @Deprecated
    public TAllocation(int vmId, Task task, double startTime) {
		this.vm = null;
		this.task = task;
		this.startTime = startTime;
		this.finishTime = startTime + task.getTaskSize() / VM.SPEEDS[vmId];
	}
    @Deprecated
	public void setVM(VM vm) {
		this.vm = vm;
	}
	//-------------------------------------only for ICPCP---------------------------
}