package cloud.workflowScheduling.contentionAware;

import java.util.*;
import java.util.Map.*;

import cloud.workflowScheduling.contentionFree.*;
import cloud.workflowScheduling.setting.*;

/**
 * Contention-aware Solution, including edge scheduling information besides task information
 * @author wu
 */
public class CSolution extends Solution{
	// The class Solution stores task scheduling information in mapping and revMapping. 
	// It stores all edge allocations on the incoming bandwidth of each VM
	private HashMap<VM, List<EAllocation>> eaInMap = new HashMap<VM, List<EAllocation>>(); 
	// It stores all edge allocations on the outgoing bandwidth of each VM
	private HashMap<VM, List<EAllocation>> eaOutMap = new HashMap<VM, List<EAllocation>>(); 
	
	private int internalVMId = 0; // next VM id in this solution
	public VM newVM(int type){
		VM v = new VM(internalVMId++, type);
		mapping.put(v, new LinkedList<TAllocation>());
		eaInMap.put(v, new LinkedList<EAllocation>());
		eaOutMap.put(v, new LinkedList<EAllocation>());
		return v;
	}
	public void removeEmptyVMs(){
		for(Iterator<Entry<VM, List<TAllocation>>> it = mapping.entrySet().iterator(); it.hasNext();){
			Entry<VM, List<TAllocation>> entry = it.next();
			if(entry.getValue().size() == 0){
				it.remove();
				eaInMap.remove(entry.getKey());
				eaOutMap.remove(entry.getKey());
			}
		}
	}
	
	public void addEdge(EAllocation ea) {
		addEdgeOneSide(eaOutMap, ea, ea.getSourceVM());
		addEdgeOneSide(eaInMap, ea, ea.getDestVM());
	}

	private void addEdgeOneSide(HashMap<VM, List<EAllocation>> eMap, EAllocation ea, VM vm) {
		if(eMap.get(vm) == null)
			eMap.put(vm, new ArrayList<EAllocation>());
		
		List<EAllocation> eaList = eMap.get(vm);
		for(int i = 0; i<=eaList.size(); i++){
			if(i==eaList.size()){
				eaList.add(i, ea);
				break;		//���break�Ǳ���ӵģ�����<=������һֱ������
			}else if(ea.getStartTime() < eaList.get(i).getStartTime()	// ||������������ִ��ʱ��Ϊ0�ı�
					|| (ea.getStartTime() == eaList.get(i).getStartTime()&&	
						ea.getFinishTime() < eaList.get(i).getFinishTime())){
				//��ΪAdaptor�����ݱߴ����ǿ���overlap�ģ����԰����¼��ȡ����
//				if(Config.isDebug() && ea.getFinishTime() > eaList.get(i).getStartTime()) {
//					System.out.println(ea);
//					throw new RuntimeException("Critical Error: EAllocation conflicts");
//				}
				eaList.add(i, ea);
				break;
			}
		}
	}
	public void removeEdge(EAllocation ea) {
		eaInMap.get(ea.getDestVM()).remove(ea);
		eaOutMap.get(ea.getSourceVM()).remove(ea);
	}
	
	//--------------------------------getters----------------------------------
	public List<EAllocation> getEAInList(VM vm) {
		List<EAllocation> eaInlist = eaInMap.get(vm);
		return eaInlist == null ? null : Collections.unmodifiableList(eaInlist);
	}
	
	public List<EAllocation> getEAOutList(VM vm) {
		List<EAllocation> eaOutlist = eaOutMap.get(vm);
		return eaOutlist == null ? null : Collections.unmodifiableList(eaOutlist);
	}
	
	public HashMap<Edge, EAllocation> getEAMap(){
		HashMap<Edge, EAllocation> map = new HashMap<>();
		for(List<EAllocation> list : eaInMap.values())
			for(EAllocation ea : list)
				map.put(ea.getEdge(), ea);
		return map;
	}

	//--------------------------------validate----------------------------------
	// a solution yielded by Adaptor.buildFromSolutionShared usually can not pass this validation
	// because the function isOverlap usually return true
	public boolean validate(Workflow wf){
		if(super.validate(wf) == false)
			return false;
		if(isOverlap(eaInMap))
			return false;		
		if(isOverlap(eaOutMap))
			return false;		
		return true;
	}
	public boolean validateTasks(Workflow wf){
		if(super.validate(wf) == false)
			return false;
		return true;
	}
	//��⴫��ʱ�����Ƿ���overlap��ע�⣬��shared��ʽ��������CSolution��ͨ��������overlap�ġ�
	private boolean isOverlap(HashMap<VM, List<EAllocation>> map){
		for(VM vm : map.keySet()){
			List<EAllocation> la = map.get(vm);
//			Collections.sort(la);
			for(int i = 1;i<la.size();i++){
				EAllocation alloc = la.get(i);
				EAllocation allocPrev = la.get(i-1);
				if(allocPrev.getFinishTime()>alloc.getStartTime() &&
						alloc.getFinishTime() != alloc.getStartTime() ){//��alloc�����ʼʱ��ͽ���ʱ��һ����û��ϵ
					System.out.println("edge overlap on " + vm.getId() + ": "
						 + allocPrev + "\t" + alloc);
					return true;
				}
			}
		}
		return false;
	}
}