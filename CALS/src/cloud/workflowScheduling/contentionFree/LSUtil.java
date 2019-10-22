package cloud.workflowScheduling.contentionFree;

import java.util.*;

import cloud.workflowScheduling.setting.*;
import cloud.workflowScheduling.util.*;

/**
 * a utility class for list scheduling
 * @author wu
 */
public class LSUtil {
	
	// two benchmark solutions, used by constrained and multiple optimization
	private Solution cheapSchedule, fastSchedule;
	
	public LSUtil(Workflow wf) {
		List<Task> tasks = new ArrayList<Task>(wf.getTaskList());
		Collections.sort(tasks, new TProperties(wf, TProperties.Type.B_LEVEL)); //sort based on bLevel
		Collections.reverse(tasks); 	// larger first

		//list scheduling based on bLevel and EST; a kind of HEFT，VM固定为FASTEST且可以任意多个，求取近似的最快时间
		fastSchedule =  step2(tasks, 1);
		
		// in one slowest VM, use EST to allocate tasks
		cheapSchedule = new Solution();
		VM vm = cheapSchedule.newVM(VM.SLOWEST);//uses one slowest VM
		for(Task task : tasks){
			double EST = calcEST(cheapSchedule, task, vm);
			cheapSchedule.addTaskToVMEnd(vm, task, EST);
		}
	}
	public Solution listScheduling(Workflow wf, TProperties.Type ttype, int type){
		List<Task> tasks = new ArrayList<Task>(wf.getTaskList());
		Collections.sort(tasks, new TProperties(wf, ttype)); //sort based on bLevel
		Collections.reverse(tasks); 	// larger first

		return step2(tasks, type);
	}
	
	/**
	 * step2 for list scheduling: resource allocation for @param tasks
	 * @param type = 0: none
	 * 		  type = 1: insert is supported
	 * 		  type = 2: insert and duplication are supported
	 */
	private Solution step2(List<Task> tasks, int type) {
		final Solution solution = new Solution();
		for(int i = 0; i < tasks.size(); i++){		
			Task task = tasks.get(i);				//select VM based on EST
			
			double minEST = Double.MAX_VALUE;
			VM selectedVM = null;
			List<TAllocation> selectedDupList = null;
			
			List<VM> vmList = buildVMList(solution);
			// calculate EST of task on all the used VMs
			List<TAllocation> dupList = new ArrayList<TAllocation>();
			for(VM vm : vmList){			
				dupList.clear();
				double EST = 0;		//若运行速度异构，得使用EFT
				switch(type){
				case 0: EST = calcEST(solution, task, vm); break;
				case 1: EST = calcESTWithInsert(solution, task, vm);break;
				case 2: EST = calcESTWithDup(solution, task, vm, dupList);break;
				}
				if(EST<minEST){
					minEST = EST;
					selectedVM = vm;
					selectedDupList = new ArrayList<TAllocation>(dupList);
				}
			}
			if(selectedDupList != null)
				for(TAllocation alloc : selectedDupList){
					solution.addTaskToVM(alloc.getVM(), alloc.getTask(), alloc.getStartTime());
					System.out.println("存在复制！");
				}
			solution.addTaskToVM(selectedVM, task, minEST);	//allocation	
		}
		return solution;
	}

	/**
	 * step2 for list scheduling: resource allocation for @param tasks
	 * @param insertFlag represents whether insert is supported
	 * 该函数的功能被step2(List<Task> tasks, int type)基本包含，但先不删除，因为简单留着做个参考
	 * Another difference with step2(List<Task> tasks, int type) is on the empty VM
	 */
	private Solution step2(List<Task> tasks, boolean insertFlag) {
		Solution solution = new Solution();
		for(int i = 0; i < tasks.size(); i++){		
			Task task = tasks.get(i);				//select VM based on EST
			double minEST = Double.MAX_VALUE;
			VM selectedVM = null;
			for(VM vm : solution.getUsedVMSet()){	// calculate EST of task on all the used VMs
				double EST = insertFlag?calcESTWithInsert(solution,task, vm):calcEST(solution,task, vm);
				if(EST<minEST){
					minEST = EST;
					selectedVM = vm;
				}
			}
			//whether minEST can be shorten if a new vm is added
			double EST = calcEST(solution, task, null);	
			if(EST < minEST){
				minEST = EST;
				selectedVM = solution.newVM(VM.FASTEST);
			}
//			if(i == 1)		//before allocating task_1, allocate entryTask to the same VM 
//				solution.addTaskToVMEnd(selectedVM, tasks.get(0), minEST);
			solution.addTaskToVM(selectedVM, task, minEST);	//allocation	addTaskToVMEnd
		}
		return solution;
	}

	/**
	 * prepare a VM list for the 2nd step of list scheduling
	 * one empty VM is needed and empty VMs are put behind
	 */
	public static List<VM> buildVMList(final Solution solution) {
		boolean emptyVMFlag = false;	//始终保持一个new vm
		for(VM vm : solution.getUsedVMSet())
			if(solution.getTAListOnVM(vm)==null||solution.getTAListOnVM(vm).size()==0)
				emptyVMFlag = true;
		if(!emptyVMFlag)
			solution.newVM(VM.FASTEST);		
		
		List<VM> vmList = new ArrayList<VM>(solution.getUsedVMSet());
		Collections.sort(vmList, new Comparator<VM>(){
			public int compare(VM v1, VM v2) {
				if(solution.getTAListOnVM(v1)==null||solution.getTAListOnVM(v1).size()==0)
					return 1;
				if(solution.getTAListOnVM(v2)==null||solution.getTAListOnVM(v2).size()==0)
					return -1;
				return 0;
			}
		});		//空的vm必须放后面尝试
		return vmList;
	}
	
	//--------------以下四个函数层层递进关系：calcDAT、calcEST、calcESTWithInsert、calcESTWithDup------------
	/**
	 * calculate data arrival time (DAT) of @param task on @param vm for @param solution
	 */
	private static double calcDAT(Solution solution, Task task, VM vm) {
		double DAT = 0; 		
		for(Edge inEdge : task.getInEdges()){
			Task parent = inEdge.getSource();
			List<TAllocation> parentAllocs = solution.getTAList(parent);
			if(parentAllocs == null)	//entryTask
				continue;
			double minArrivalTime = Double.MAX_VALUE;
			for(TAllocation parentAlloc : parentAllocs){
				double arrivalTime = parentAlloc.getFinishTime();
				if(parentAlloc.getVM() != vm )
					arrivalTime += inEdge.getDataSize() / VM.NETWORK_SPEED;
				minArrivalTime = Math.min(minArrivalTime, arrivalTime);
			}
			DAT = Math.max(DAT, minArrivalTime);
		}
		return DAT;
	}
	
	/**
	 * calculate Earliest Starting Time of @param task on @param vm for @param solution
	 */
	public static double calcEST(Solution solution,Task task, VM vm){
		double DAT = calcDAT(solution, task, vm);		// DAT: data arrival/ready time	
		double EST = Math.max(DAT, VM.LAUNCH_TIME);   // VM==null
		if(vm != null){
			EST = Math.max(DAT, solution.getVMFinishTime(vm));
		}
		return EST;
	}
	
	/**
	 * calcEST when insert is enabled
	 */
	public static double calcESTWithInsert(Solution solution, Task task, VM vm){
		if(vm == null)
			throw new RuntimeException("null VM");
		double DAT = calcDAT(solution, task, vm);		// DAT: data arrival/ready time	
		
		double period = task.getTaskSize()/vm.getSpeed();
		List<TAllocation> allocs = solution.getTAListOnVM(vm);
		double EST = Allocation.searchFreeTimeSlot(allocs, DAT, period);
		return EST;
	}
	
	/**
	 * calcEST when insert and duplication are both enabled
	 */
	public static double calcESTWithDup(Solution solution, Task task, VM vm, List<TAllocation> dupList){
		double EST = calcESTWithInsert(solution, task, vm);
		
		//Sort parents vj  not in vm by their DAT 
		List<Task> parentsNotInThisVM = solution.getParentsNotInVM(task, vm);
		for(Task parent : parentsNotInThisVM){
//			Task parent = e.getSource();
			double parentEST = calcESTWithInsert(solution, parent, vm);
			TAllocation parentAlloc = solution.addTaskToVM(vm, parent, parentEST);
			double newEST = calcESTWithInsert(solution, task, vm);
			if(newEST<EST){
				EST = newEST;
				dupList.add(parentAlloc); //记录到dupList当中
			}else{		//若没用，立马撤销
				solution.removeAllocation(parentAlloc);
			}
		}
		for(TAllocation parentAlloc : dupList)	{	//dupList当中的全部撤销插入
			solution.removeAllocation(parentAlloc);
		}
		return EST;
	}

	
	/**
	 * dynamic list scheduling for @param wf
	 */
	public Solution dynamicLS(Workflow wf){
		Solution s = new Solution();
		
		List<Task> readyTasks = new ArrayList<>();
		readyTasks.add(wf.getEntryTask());
		HashMap<Task, Integer> inEdgeCounts = new HashMap<>();	//count # of inEdges for each task

		TProperties blevels = new TProperties(wf, TProperties.Type.B_LEVEL);
		while(readyTasks.size()>0){
			Task selectedTask = null;
			VM selectedVM = null;
			double minEST =  Double.MAX_VALUE;		
			List<VM> vmList = LSUtil.buildVMList(s);
			readyTasks.sort(blevels);
			Collections.reverse(readyTasks);// ties are broken by selecting the task with a larger b-level
			for(Task task : readyTasks){
				for(VM vl : vmList){
					double tsTask = calcESTWithInsert(s,task, vl);
					if(tsTask < minEST){
						minEST = tsTask;
						selectedVM = vl;
						selectedTask = task;
					}
				}
			}
			s.addTaskToVM(selectedVM, selectedTask, minEST);	
			
			readyTasks.remove(selectedTask);
			for(Edge e : selectedTask.getOutEdges()){
				Task dTask = e.getDestination();
				Integer count = inEdgeCounts.get(dTask);
				inEdgeCounts.put(dTask, count != null ? count+1 : 1);
				if(inEdgeCounts.get(dTask) == dTask.getInEdges().size()){
					readyTasks.add(dTask);
				}
			}
		}
		s.removeEmptyVMs();
		return s;
	}
	
	//----------------------------getters-------------------------------------
	public Solution getCheapSchedule() {
		return cheapSchedule;
	}
	public Solution getFastSchedule() {
		return fastSchedule;
	}
	
	// local test 测试不同方法对于makespan优化的效果
	public static void main(String[] args){
		//D:\\test.dot					结果：	505 411
		//F:\\dax\\floodplain.xml				23400
		//F:\\dax\\SIPHT\\SIPHT.n.100.1.dax		1019
		//F:\\dax\\CYBERSHAKE\\CYBERSHAKE.n.100.1.dax	62.64 51.95	
    	String file = "D:\\test.dot";	
    	Workflow wf = new Workflow(file);	
    	
    	LSUtil ls = new LSUtil(wf);
		List<Task> tasks = new ArrayList<Task>(wf.getTaskList());
		Collections.sort(tasks, new TProperties(wf, TProperties.Type.B_LEVEL));
		Collections.reverse(tasks); 	// larger first
    	
		List<Solution>	list = new ArrayList<Solution>();
		list.add(ls.getCheapSchedule());
		list.add(ls.getFastSchedule());
//		list.add(ls.step2(tasks, false));
//		list.add(ls.step2(tasks, true));
		list.add(ls.step2(tasks, 0));
		list.add(ls.step2(tasks, 1));
		list.add(ls.step2(tasks, 2));
		list.add(ls.dynamicLS(wf));
    	
		for(Solution s:list){
			System.out.println(s.validate(wf));
			ChartUtil.visualizeScheduleNew(s);
		}
		for(Solution s:list){
			System.out.println(s.getMakespan()+"\t"+s.getCost());
		}
	}
}