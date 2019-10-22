package cloud.workflowScheduling.contentionFree;

import java.util.*;
import java.util.Map.*;

import cloud.workflowScheduling.setting.*;

// This class has been greatly refactored
/**
 * This class represents a schedule solution in the contention-oblivious environment.
 * That is, it only includes task scheduling information
 * It can be a partial / intermediate solution, or a whole one
 * @author wu
 */
public class Solution {

	//TAllocation List is sorted based on startTime
	//因为一个task可能被复制，所以一个task在一个solution中可有多个allocation
	protected HashMap<VM, List<TAllocation>> mapping = new HashMap<>();

	//reverseMapping: the content in revMapping is the same as that in mapping
	//used to make get_Allocation_by_Task easy 
	protected HashMap<Task, List<TAllocation>> revMapping = new HashMap<>();

	private double[] values = null;
	private int internalVMId = 0;
	
	public VM newVM(int type){
		VM vm = new VM(internalVMId++, type);
		mapping.put(vm, new LinkedList<TAllocation>());
		return vm;
	}
	public void removeEmptyVMs(){
		for(Iterator<Entry<VM, List<TAllocation>>> it = mapping.entrySet().iterator(); it.hasNext();){
			Entry<VM, List<TAllocation>> entry = it.next();
			if(entry.getValue().size() == 0)
				it.remove();
		}
	}
	
	//------------------------------add / remove a task allocation----------------------------
	// index = -1表示直接插在结尾;  返回的allocation是为了方便撤销插入
	private TAllocation addTaskToVMWithIndex(VM vm, Task task, double startTime, int index){
		if(mapping.containsKey(vm) == false)
			mapping.put(vm, new LinkedList<TAllocation>());
		
		TAllocation alloc = new TAllocation(vm, task, startTime);
		if(Config.isDebug()) {
			List<TAllocation> list = mapping.get(vm);
			double startTime1 = alloc.getStartTime();
			double finishTime1 = alloc.getFinishTime();
			boolean conflict = false;				//check whether there is time conflict
			for(TAllocation prevAlloc : list){
				double startTime2 = prevAlloc.getStartTime();
				double finishTime2 = prevAlloc.getFinishTime();
				if((startTime1>startTime2 && startTime2>finishTime1)	  //startTime2 is between startTime1 and finishTime1
					|| (startTime2>startTime1 && startTime1>finishTime2)) //startTime1 is between startTime2 and finishTime2
					conflict = true;
			}
			if(conflict)
				throw new RuntimeException("Critical Error: TAllocation conflicts");
		}
		if(index == -1)
			mapping.get(vm).add(alloc);
		else
			mapping.get(vm).add(index, alloc);

		if(revMapping.get(task) == null)
			revMapping.put(task, new ArrayList<TAllocation>());
		revMapping.get(task).add(alloc);
		return alloc;
	}

	public TAllocation addTaskToVMEnd(VM vm, Task task, double startTime){
		return addTaskToVMWithIndex(vm, task, startTime, -1);
	}
	public TAllocation addTaskToVMStart(VM vm, Task task, double startTime){
		return addTaskToVMWithIndex(vm, task, startTime, 0);
	}
	//寻找指定位置进行插入，分为三种情况：1.vm是空的；2.vm里能插入；3.若不能插入则在最后
	public TAllocation addTaskToVM(VM vm, Task task, double startTime){
		if(mapping.containsKey(vm) == false )		
			return addTaskToVMWithIndex(vm, task, startTime, -1);

		List<TAllocation> allocations = mapping.get(vm);
		for(int i = 0; i<allocations.size(); i++){		//插入
			TAllocation curAlloc = allocations.get(i);
			if(startTime < curAlloc.getStartTime()||
				(startTime == curAlloc.getStartTime()&&task.getTaskSize()==0)){	
				//第二个条件是因为如entry等大小为0的任务的存在，必须把他们这么放curAlloc之前
				return addTaskToVMWithIndex(vm, task, startTime, i);
			}
		}
		return addTaskToVMWithIndex(vm, task, startTime, -1);	//最后
	}

	public void removeAllocation(TAllocation alloc){
		mapping.get(alloc.getVM()).remove(alloc);
		revMapping.get(alloc.getTask()).remove(alloc);
	}
		
	//----------------------------------------getters-------------------------------------------
	public List<TAllocation> getTAList(Task t) {
		List<TAllocation> taList = revMapping.get(t);
		return taList==null ? null : Collections.unmodifiableList(taList);
	}
	public TAllocation getFirstTA(Task t) {
		List<TAllocation> taList = revMapping.get(t);
		if(taList == null || taList.size()==0)
			throw new RuntimeException("getFirstTa fails");
		return taList.get(0);
	}
	public List<TAllocation> getTAListOnVM(VM vm){
		List<TAllocation> taList = mapping.get(vm);
		return taList==null ? null : Collections.unmodifiableList(taList);
	}
	public Set<Task> getAllocatedTaskSet() {
		Set<Task> allocatedTaskSet = revMapping.keySet();
		return allocatedTaskSet==null ? null : Collections.unmodifiableSet(allocatedTaskSet);
	}
	public Set<VM> getUsedVMSet(){
		Set<VM> usedVMs = mapping.keySet();
		return usedVMs == null ? null : Collections.unmodifiableSet(usedVMs);
	}
	public void sortTAListOnVM(VM vm, Comparator<TAllocation> comp){	//ICPCP中需要
		List<TAllocation> taList = mapping.get(vm);
		Collections.sort(taList, comp);
	}
	/**
	 * get parent tasks of @param task which are not located in @param vm
	 */
	public List<Task> getParentsNotInVM( Task task, VM vm) {
		List<Edge> edgesNotInThisVM = new ArrayList<Edge>();
		for(Edge edge : task.getInEdges()){
			Task parent = edge.getSource();
			boolean inFlag = false;
			if(this.getTAList(parent)!=null){		//entry task
				for(TAllocation alloc : this.getTAList(parent))
					if(alloc.getVM()==vm)
						inFlag = true;
				if(!inFlag)
					edgesNotInThisVM.add(edge);
			}
		}
		Collections.sort(edgesNotInThisVM, new Comparator<Edge>(){
			public int compare(Edge e1, Edge e2) {
				Task p1 = e1.getSource();
				Task p2 = e2.getSource();
				if(Solution.this.getTAList(p1)==null || Solution.this.getTAList(p2)==null)
					return 0;
				//只取allocation list当中的第一个就够了，因为肯定是第一个allocation的执行时间最早
				double dat1 = Solution.this.getFirstTA(p1).getFinishTime();
				double dat2 = Solution.this.getFirstTA(p2).getFinishTime();
				dat1 += e1.getDataSize()/VM.NETWORK_SPEED;
				dat2 += e2.getDataSize()/VM.NETWORK_SPEED;
				return -1 * Double.compare(dat1, dat2);
			}
		});
		List<Task> parentsNotInThisVM = new ArrayList<>();
		for(Edge edge : edgesNotInThisVM)
			parentsNotInThisVM.add(edge.getSource());
		return parentsNotInThisVM;
	}

	//----------------------------getters based on calculation----------------------------------
	public double getCost(){
		double totalCost = 0;
		for(VM vm : mapping.keySet()){
			double vmCost = getVMCost(vm); 
			totalCost += vmCost;
		}
		return totalCost;
	}
	public double getVMCost(VM vm){
		return vm.getUnitCost() * 
				Math.ceil((this.getVMLeaseEndTime(vm) - this.getVMLeaseStartTime(vm))/VM.INTERVAL);
	}
	
	public double getMakespan(){		//也可以是部分解的makespan
		double makespan = -1;
		for(VM vm : mapping.keySet()){
			makespan = Math.max(this.getVMFinishTime(vm), makespan); //finish time of the last task
		}
		return makespan;
	}
	public double[] getValues(){
		if(values == null){
			double[] vs = {getMakespan(), getCost()};
			values = vs;
		}
		return values;
	}
	
	//这种计算方式有出现不准确的可能
	//VM's lease start time and finish time are calculated based on allocations
	public double getVMLeaseStartTime(VM vm){	
		if(mapping.get(vm).size() == 0)
			return VM.LAUNCH_TIME;
		else{
			Task firstTask = mapping.get(vm).get(0).getTask();
			double ftStartTime = mapping.get(vm).get(0).getStartTime(); // startTime of first task
			
			double maxTransferTime = 0;
			for(Edge e : firstTask.getInEdges()){
				//因为是第一个任务，不再判断e.getSource()是否和当前的task处于同一个vm
				maxTransferTime = Math.max(maxTransferTime, e.getDataSize() / VM.NETWORK_SPEED);
			}
			return ftStartTime - maxTransferTime;
		}
	}
	public double getVMLeaseEndTime(VM vm){
		if(mapping.get(vm)== null || mapping.get(vm).size() == 0)
			return VM.LAUNCH_TIME;
		else{
			List<TAllocation> allocations = mapping.get(vm);
			
			TAllocation lastAlloc = allocations.get(allocations.size()-1);
			double ltFinishTime = lastAlloc.getFinishTime(); // finishTime of last task
			
			double maxTransferTime = 0;
			for(Edge e : lastAlloc.getTask().getOutEdges()){
				maxTransferTime = Math.max(maxTransferTime, e.getDataSize() / VM.NETWORK_SPEED);
			}
			return ltFinishTime + maxTransferTime;
		}
	}
	public double getVMFinishTime(VM vm){		//finish time of the last task
		if(mapping.get(vm)== null || mapping.get(vm).size() == 0)
			return VM.LAUNCH_TIME;
		else{
			List<TAllocation> allocations = mapping.get(vm);
			return allocations.get(allocations.size()-1).getFinishTime(); 
		}
	}

	//----------------------------------------others-------------------------------------------
	//check whether there is time conflict in this schedule solution
	//非常严格的检查，若通过了Solution肯定是valid的
	public boolean validate(Workflow wf){
		//任务VM执行上是否有overlap?
		for(VM vm : mapping.keySet()){
			List<TAllocation> la = mapping.get(vm);
//			Collections.sort(la);
			for(int i = 1;i<la.size();i++){
				TAllocation alloc = la.get(i);
				TAllocation allocPrev = la.get(i-1);
				if(allocPrev.getFinishTime()>alloc.getStartTime()){
					System.out.println("task overlap" + allocPrev.getFinishTime()+"\t"+alloc.getStartTime());
					return false;
				}
			}
		}
		
		List<TAllocation> allAllocList = new ArrayList<TAllocation>();
		Set<Task> taskSet = new HashSet<Task>();
		for(List<TAllocation> allocList : revMapping.values()){
			for(TAllocation alloc : allocList){
				taskSet.add(alloc.getTask());
				allAllocList.add(alloc);
			}
		}
		//check # of tasks
		if(taskSet.size() != wf.size())	{
			System.out.println("task size problem: " + taskSet.size() + "\t" + wf.size());
			return false;
		}
		//边是否满足要求，检测是否是拓扑排序
//		Collections.sort(list);			
		for(TAllocation alloc : allAllocList){
			Task task = alloc.getTask();	// check each task and its parents
			for(Edge e : task.getInEdges()){
				Task parentTask = e.getSource();
				
				List<TAllocation> parentAllocList = this.revMapping.get(parentTask);
				boolean isValid = false;
				for(TAllocation parentAlloc : parentAllocList){
					if(alloc.getVM() != parentAlloc.getVM() 				
							&& parentAlloc.getFinishTime() +e.getDataSize()/VM.NETWORK_SPEED
							<= alloc.getStartTime()+ Config.EPS)
						isValid = true;
					else if(alloc.getVM() == parentAlloc.getVM() 				
							&& parentAlloc.getFinishTime() <= alloc.getStartTime() + Config.EPS)
						isValid = true;
				}
				if(isValid == false){
					System.out.println("edge precedency problem: " + alloc + "\t" +parentAllocList.get(0));
					return false;
				}
			}
		}
		return true;
	}
	
	//----------------------------------------override-------------------------------------------
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(" " + this.getCost() + "\t" + this.getMakespan()+"\r\n");
		for(VM vm : mapping.keySet()){
			sb.append(vm.toString() + mapping.get(vm).toString()+"\r\n");
		}
		return sb.toString();
	}
	
	@Override	
	public Object clone() {		//只能以这样的方式进行clone
		Solution s = new Solution();
		s.internalVMId = this.internalVMId;
		for(VM vm : mapping.keySet()){
			List<TAllocation> list = mapping.get(vm);
			for(TAllocation alloc : list)
				s.addTaskToVMEnd(vm, alloc.getTask(), alloc.getStartTime());
		}
		return s;
	}
}