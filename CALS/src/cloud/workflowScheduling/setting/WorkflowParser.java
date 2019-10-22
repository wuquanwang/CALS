package cloud.workflowScheduling.setting;

import java.io.*;
import java.util.*;
import java.util.regex.*;

import javax.xml.parsers.*;

import org.xml.sax.*;
import org.xml.sax.helpers.*;

/**
 * This class is used for parsing workflow files: 1. DOT graphs; 2. Pegasus dax files
 * @author wu
 */
public class WorkflowParser {
	public static List<Task> parseXMLFile(String file) throws Exception{
		List<Task> list = new ArrayList<>();
		
		MyDAXReader daxReader = new MyDAXReader();
		SAXParser sp = SAXParserFactory.newInstance().newSAXParser();
		sp.parse(new InputSource(file), daxReader);
		for(Task t: daxReader.getNameTaskMapping().values())
			list.add(t);
		addDummyTasks(list);
		
		// Bind data flow to control flow, i.e., setDataSize for an edge
		HashMap<String, TransferData> transferData = daxReader.getTransferData();
		Task tentry = list.get(0);
		Task texit = list.get(list.size() - 1);
		for(TransferData td : transferData.values()){	//Bind data flow to control flow
			Task source = td.getSource();
			List<Task> destinations = td.getDestinations();
			if(source == null){
				source = tentry;
				td.setSize(0);		//a setting: transfer time of input data is omitted --- setting to 0
			}
			if( destinations.size()==0)	
				destinations.add(texit);
			for(Task destination : destinations){
				boolean flag = true;
				for(Edge outEdge : source.getOutEdges()){
					if(outEdge.getDestination() == destination){
						outEdge.setDataSize(td.getSize());			//bind here
						flag = false;
					}
				}
				//an annoying problem in some DAX files: a data flow cannot be bound to existing control flows
				//flag to indicate whether this problem exists
				if(flag == true){
					Edge e = new Edge(source, destination);
					e.setDataSize(td.getSize());
					source.insertEdge(Task.TEdges.OUT, e);
					destination.insertEdge(Task.TEdges.IN, e);
					System.out.println("**************add a control flow*******************source: "
							+e.getSource().getName()+"; destination: "+e.getDestination().getName());
				}
			}
		}
		return list;
	}
	
	//note weight values here are multiplied by VM.SPEEDS or NETWORK_SPEED
	public static List<Task> parseDOTFile(String file) throws IOException {
		List<Task> list = new ArrayList<>();
		HashMap<String, Task> nameTaskMapping = new HashMap<String, Task>();
		
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line;
        String vertexP = "([0-9A-Z]*)\\s*\\[label=\"\\1\\:([0-9]*)";
		String edgeP = "([0-9A-Z]*)\\s*->\\s*([0-9A-Z]*)\\s*\\[label=([0-9]*)\\];";
		Pattern vertexPattern = Pattern.compile(vertexP);
		Pattern edgePattern = Pattern.compile(edgeP);
		while((line=br.readLine())!=null){
			Matcher edgeM = edgePattern.matcher(line);
			Matcher vertexM = vertexPattern.matcher(line);
			if(vertexM.find()){
				String name = vertexM.group(1);
				double weight = Double.parseDouble(vertexM.group(2));
				Task t = new Task(name, weight*VM.SPEEDS[VM.FASTEST]);
				nameTaskMapping.put(name, t);
			}else if(edgeM.find()){
				String name1 = edgeM.group(1);
				String name2 = edgeM.group(2);
				double weight = Double.parseDouble(edgeM.group(3));
				Task task1 = nameTaskMapping.get(name1);
				Task task2 = nameTaskMapping.get(name2);
				Edge e = new Edge(task1, task2);
				e.setDataSize((long)(weight*VM.NETWORK_SPEED));
				task1.insertEdge(Task.TEdges.OUT, e);
				task2.insertEdge(Task.TEdges.IN, e);
			}
		}
		for(Task t: nameTaskMapping.values())
			list.add(t);
		addDummyTasks(list);
		br.close();
		
		return list;
	}
	
	//-----------add two dummy tasks to this workflow----------------------
	// 注意，这个过程必须放在Parser类中，因为parse XML时，源和目的任务还有用的。
	private static void addDummyTasks(List<Task> list) {
		Task tentry = new Task(("entry"), 0);	
		Task texit = new Task(("exit"), 0);
		for(Task t: list){						//add edges to entry and exit
			if(t.getInEdges().size()==0){
				Edge e = new Edge(tentry, t);
				t.insertEdge(Task.TEdges.IN, e);
				tentry.insertEdge(Task.TEdges.OUT, e);
			}
			if(t.getOutEdges().size()==0){
				Edge e = new Edge(t, texit);
				t.insertEdge(Task.TEdges.OUT, e);
				texit.insertEdge(Task.TEdges.IN, e);
			}
		}
		list.add(0, tentry);			//add the entry and exit nodes to the workflows
		list.add(texit);
	}
}

class MyDAXReader extends DefaultHandler{		//this class is only used in parsing DAX data
	private HashMap<String, Task> nameTaskMapping = new HashMap<String, Task>();
	private HashMap<String, TransferData> transferData = new HashMap<String, TransferData>(); //前提： fileName必须可作为标示

	private Stack<String> tags = new Stack<String>();
	private String childId;
	private Task lastTask;
	public void startElement(String uri, String localName, String qName, Attributes attrs) {
		if(qName.equals("job")){
			String id = attrs.getValue("id");
			if(nameTaskMapping.containsKey(id))		//id conflicts
				throw new RuntimeException();
			Task t = new Task(id, Double.parseDouble(attrs.getValue("runtime")));
			nameTaskMapping.put(id, t);
			lastTask = t;
		}else if(qName.equals("uses") && tags.peek().equals("job")){
			//After reading the element "job", the element "uses" means a trasferData (i.e., data flow)
			String filename =attrs.getValue("file");
				
			long fileSize = Long.parseLong(attrs.getValue("size"));
			TransferData td = transferData.get(filename);
			if(td == null){
				td = new TransferData(filename, fileSize);
			}
			if(attrs.getValue("link").equals("input")){
				td.addDestination(lastTask);
			}else{									//output
				td.setSource(lastTask);
			}
			transferData.put(filename, td);
		}else if(qName.equals("child") ){		
			childId = attrs.getValue("ref");
		}else if(qName.equals("parent") ){ 
			//After reading the element "child", the element "parent" means an edge (i.e., control flow)
			Task child = nameTaskMapping.get(childId);
			Task parent = nameTaskMapping.get(attrs.getValue("ref"));
			
			Edge e = new Edge(parent, child);			//control flow
			parent.insertEdge(Task.TEdges.OUT, e);
			child.insertEdge(Task.TEdges.IN, e);
		}
		tags.push(qName);
	}
	public void endElement(String uri, String localName,String qName) {
		tags.pop();
	}
	public HashMap<String, Task> getNameTaskMapping() {
		return nameTaskMapping;
	}
	public HashMap<String, TransferData> getTransferData() {
		return transferData;
	}
}

class TransferData{		//this class is only used in parsing DAX data
	private String name;
	private long size;
	private Task source;		//used to bind control flow and data flow
	private List<Task> destinations = new ArrayList<Task>();

	public TransferData(String name, long size) {
		this.name = name;
		this.size = size;
	}
	
	//-------------------------------------getters & setter--------------------------------
	public long getSize() {return size;}
	public Task getSource() {return source;}
	public void setSource(Task source) {this.source = source;}
	public void addDestination(Task t){destinations.add(t);}
	public List<Task> getDestinations() {return destinations;}
	public void setSize(long size) {
		this.size = size;
	}
	//-------------------------------------overrides--------------------------------
	public String toString() {return "TransferData [name=" + name + ", size=" + size + "]";}
}
