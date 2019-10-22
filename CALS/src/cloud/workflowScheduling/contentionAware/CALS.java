package cloud.workflowScheduling.contentionAware;

import java.io.*;
import java.util.*;

import cloud.workflowScheduling.contentionFree.*;
import cloud.workflowScheduling.setting.*;
import cloud.workflowScheduling.util.*;

/**
 * contention-aware list scheduling
 * @author wu
 */
public class CALS {
	
	private CSolution csolution;

	/**
	 * static list scheduling for @param wf
	 * @param type = 0: none
	 * 		  type = 1: rescheduling is enabled
	 * 		  type = 2: duplication is supported
	 * besides, insert is always supported
	 */
	public CSolution listSchedule(Workflow wf, TProperties.Type tctype, int type) {
		List<Task> tasks = new ArrayList<Task>(wf.getTaskList());
		Collections.sort(tasks, new TProperties(wf, tctype)); //sort based on tctype
		Collections.reverse(tasks); 	// larger first		
		
		csolution = new CSolution();
		for(int i = 0; i < tasks.size(); i++){		
			Task task = tasks.get(i);				
			List<VM> vmList = LSUtil.buildVMList(csolution);
			
			double minEST = Double.MAX_VALUE;		
			VM selectedVM = null;
			List<List<Allocation>> selectedDupList = null;
			List<List<Allocation>> dupList = new ArrayList<>();
			for(VM vl : vmList){	
				dupList.clear();
				double tsTask;
				if(type==2)
					tsTask = calcTSTaskWithDup(task, vl, dupList);
				else
					tsTask = calcTSTaskWithInsert(task, vl);
				if(tsTask < minEST){
					minEST = tsTask;
					selectedVM = vl;
					if(type==2)
						selectedDupList = new ArrayList<>(dupList);
				}
			}
			if(type==2 && selectedDupList!=null){	
				for(List<Allocation> alloc : selectedDupList){//Ҫ�Ƚ��и��ƣ��ٽ���֮��ķ��䣬����߷������Ӱ��
					TAllocation talloc = (TAllocation)(alloc.get(0));
					addTaskEdgesToVM(talloc.getVM(), talloc.getTask(), talloc.getStartTime());
				}
			}
			addTaskEdgesToVM(selectedVM, task, minEST);	//allocation
			
			//��̬����
//			ChartUtil.visualize(csolution);
//			System.out.println(task.getName());
//			try {byte[] b = new byte[2]; System.in.read(b);} catch (IOException e) {}
		}
		csolution.removeEmptyVMs();	//�Ƴ���solution��û��task��vm
		
		if(type == 1){
	    	Adaptor adaptor = new Adaptor();
	    	return adaptor.buildFromSolutionExclusive(csolution, wf, TProperties.Type.GAMMA);
		}
		return csolution;
	}
	
	//-------------------------methods used for resource allocation: begin------------------------
	/**
	 * ��Դ��������Ƶ������parentTask�ӽ��µ� transfer ready time
	 * that is��EReadyTime (edgeOutEST): max{parent's finish time, its VM's available time} 
	 */
	private TAllocation getMinEReadyTimeAlloc(Edge inEdge, VM vm) {
		Task parentTask = inEdge.getSource();
		List<TAllocation> taList = csolution.getTAList(parentTask);
		
		double minEReadyTime = Double.MAX_VALUE;
		TAllocation selected=null;
		for(TAllocation parentAlloc : taList){
			if(parentAlloc.getVM() == vm)
				return parentAlloc;
			double period = inEdge.getDataSize()/VM.NETWORK_SPEED;
			List<EAllocation> eaList = csolution.getEAOutList( parentAlloc.getVM() );
			double eReadyTime = Allocation.searchFreeTimeSlot(eaList, parentAlloc.getFinishTime(),period);
			if(eReadyTime<minEReadyTime){
				eReadyTime = minEReadyTime;
				selected = parentAlloc;
			}
		}
		return selected;
	}
	
	private double getMinEReadyTime(Edge inEdge, VM vm) {
		TAllocation parentAlloc = getMinEReadyTimeAlloc(inEdge, vm);
		double period = inEdge.getDataSize()/VM.NETWORK_SPEED;
		List<EAllocation> eaOutList = csolution.getEAOutList( parentAlloc.getVM() );
		return  Allocation.searchFreeTimeSlot(eaOutList, parentAlloc.getFinishTime(),period);
	}
	
	/**
	 * Calculate DAT of @param task on @param vm. Because of contention-awareness, 
	 * incoming edge allocations are involved and recorded in @param records.
	 * DAT: data arrival/ready time. ��Ӧ�������е��㷨2 allocateEdges(nj, vl)
	 */
	private double calcDAT(Task task, VM vm, List<EAllocation> records){
		List<Edge> inEdges = new ArrayList<Edge>(task.getInEdges());
		Collections.sort(inEdges, new Comparator<Edge>(){
			public int compare(Edge e1, Edge e2) {
				if(csolution.getTAList(e1.getSource())==null)	//entry
					return 1;
				if(csolution.getTAList(e2.getSource())==null)	//entry
					return -1;
//				double eReadyTime1 = csolution.getFirstTA(e1.getSource()).getFinishTime();
//				double eReadyTime2 = csolution.getFirstTA(e2.getSource()).getFinishTime();
				double eReadyTime1 = getMinEReadyTime(e1, vm);	//ready time 
				double eReadyTime2 = getMinEReadyTime(e2, vm);
				//�������㷨2���ts(ei,j, ��(ni));�ļ�����Ǻ�����һ���ģ�Ҳ�ǰ�������
				return Double.compare(eReadyTime1, eReadyTime2);
			}
		});//����Դ��DAT��������InEdges��	parentTask�ڸ�vm�ϵ�edge���������ﶼ����ν
		
		double DAT = 0; 		
		for(Edge inEdge : inEdges){
			Task parent = inEdge.getSource();
			if(csolution.getTAList(parent) == null)	//entryTask
				continue;
			
			TAllocation parentAlloc = getMinEReadyTimeAlloc(inEdge, vm); //Ѱ�������parentAlloc
			if(parentAlloc.getVM() == vm){
				DAT = Math.max(DAT, parentAlloc.getFinishTime());
			}else{
				double period = inEdge.getDataSize()/VM.NETWORK_SPEED;

				List<EAllocation> eaInList = csolution.getEAInList(vm);
				List<EAllocation> eaOutList = csolution.getEAOutList(parentAlloc.getVM());
				double edgeOutEST = getMinEReadyTime(inEdge, vm); 	//����˵����翪ʼ
				double edgeInEST = Allocation.searchFreeTimeSlot(eaInList, edgeOutEST, period);//��Ӧ�ļ���������Ƿ����
				while(edgeOutEST != edgeInEST){
					edgeOutEST = Allocation.searchFreeTimeSlot(eaOutList, edgeInEST, period);
					edgeInEST = Allocation.searchFreeTimeSlot(eaInList,edgeOutEST,period); 
				}
				
				//���б߷���
				EAllocation ea = new EAllocation(inEdge, parentAlloc.getVM(), vm, edgeOutEST);
				// 0���ȵı��ڷ���ʱ�����������⣬ֱ�Ӳ������ˣ� ���Ǵ�СΪ���������Ϊ���ڵ�������ϵ�����ǽ��з����˵ġ�
				if(inEdge.getDataSize()>0){		
					csolution.addEdge(ea);
					records.add(ea);
				}
				DAT = Math.max(DAT, ea.getFinishTime());
			}
		}
		for(EAllocation ea : records){	//�ع����з���
			csolution.removeEdge(ea);
		}
		return DAT;
	}
	
	/**
	 * Calculate @param task's start time on @param vl with insert enabled
	 */
	private double calcTSTaskWithInsert(Task task, VM vl) {
		double DAT = calcDAT(task, vl, new ArrayList<EAllocation>());	
		double period = task.getTaskSize()/vl.getSpeed();
		List<TAllocation> allocs = csolution.getTAListOnVM(vl);
		
		double EST = Allocation.searchFreeTimeSlot(allocs, DAT, period);
		return EST;
	}
	
	/**
	 * Calculate @param task's start time on @param vl with insert and duplication enabled
	 * �����͸����еķ���һ�£����˲��õ�calcTSTaskWithInsert������ͬ
	 */
	private double calcTSTaskWithDup(Task task, VM vm, List<List<Allocation>> dupList){
		double EST = calcTSTaskWithInsert(task, vm);
		
		List<Task> parentsNotInThisVM = csolution.getParentsNotInVM(task, vm);
		for(Task parent : parentsNotInThisVM){
			double parentEST = calcTSTaskWithInsert(parent, vm);
			List<Allocation> parentAlloc = addTaskEdgesToVM(vm, parent, parentEST);
			double newEST = calcTSTaskWithInsert(task, vm);
			if(newEST<EST){
				EST = newEST;
				dupList.add(parentAlloc); //��¼��dupList����
			}else{		//��û�ã��������������������ͱ߷���
				rollbackAdd(parentAlloc);
			}
		}
		for(List<Allocation> parentAlloc : dupList)	{	//dupList���е�ȫ����������
			rollbackAdd(parentAlloc);
		}
		return EST;
	}
	
	/**
	 * Allocate @param task on @param vm with @param startTime.
	 * Afterwards, all its incoming edges are allocated. The method calcDAT is required.
	 * @return list: the first entry is task allocation while the others are edge allocations
	 */
	private List<Allocation> addTaskEdgesToVM(VM vm, Task task, double startTime){
		TAllocation alloc = csolution.addTaskToVM(vm, task, startTime);
		List<EAllocation> eallocList = new ArrayList<EAllocation>();
		calcDAT(task, vm, eallocList);
		for(EAllocation ea : eallocList)
			csolution.addEdge(ea);

		List<Allocation> list = new ArrayList<>();
		list.add(alloc);
		list.addAll(eallocList);
		return list;		//���б��е�һ��Ϊtask allocation��ʣ���Ϊedge allocations
	}

	/**
	 * roll back task and edge allocation @param allocList
	 * the allocList should be consistent with the one returned by addTaskEdgesToVM
	 */
	private void rollbackAdd( List<Allocation> allocList) {
		TAllocation talloc = (TAllocation)(allocList.get(0));	// roll back task allocation
		csolution.removeAllocation(talloc);
		
		for(int i = 1;i<allocList.size();i++){	// roll back edge allocations
			EAllocation ealloc = (EAllocation)(allocList.get(i));
			csolution.removeEdge(ealloc);
		}
	}
	//-------------------------methods used for resource allocation: end------------------------
	
	/**
	 * dynamic list scheduling algorithm for @param wf, Ч�������Ȳ��Ͼ�̬��
	 */
	public CSolution dynamicLS(Workflow wf){
		csolution = new CSolution();
		
		List<Task> readyTasks = new ArrayList<>();
		readyTasks.add(wf.getEntryTask());
		HashMap<Task, Integer> inEdgeCounts = new HashMap<>();	//count # of inEdges for each task
		TProperties blevels = new TProperties(wf, TProperties.Type.B_LEVEL);
		
		while(readyTasks.size()>0){
			Task selectedTask = null;
			VM selectedVM = null;
			double minEST = Double.MAX_VALUE;		
			List<VM> vmList = LSUtil.buildVMList(csolution);
			readyTasks.sort(blevels);
			Collections.reverse(readyTasks); 
			for(Task task : readyTasks){
				for(VM vl : vmList){
					double tsTask = calcTSTaskWithInsert(task, vl);
					if(tsTask < minEST){
						minEST = tsTask;
						selectedVM = vl;
						selectedTask = task;
					}
				}
			}
			addTaskEdgesToVM(selectedVM, selectedTask, minEST);	
			
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
		csolution.removeEmptyVMs();
		return csolution;
	}	
}