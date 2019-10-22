package cloud.workflowScheduling.util;

import java.io.*;

import cloud.workflowScheduling.setting.*;

/**
 * With this Java class you can simply call Graphviz from your Java programs.
 * The initial code is from https://stackoverflow.com/questions/26481910/how-to-call-graphviz-from-java
 * 若选项设置不对，可能不会从DOT生成图片
 */
public class GraphViz{
	
//Detects the client's operating system. osName每次取的结果并不一样：Winnt或Win10
//private final static String osName = System.getProperty("os.name").replaceAll("\\s|\\d","");
    /**
     * Where is your dot program located? It will be called externally.
     */
	private static String DOT = Config.getDOTLocation();//"dotFor" + osName
    
    private static final int DPISize = 96;

    public static void writeDOTGraphToImageFile(String DOTgraph, String type, String file){
        try {
        	File dotTempFile = File.createTempFile("dorrr",".dot", null);
            FileWriter fout = new FileWriter(dotTempFile);
            fout.write(DOTgraph);
            fout.close();
            
            convertDOTFileToImageFile(dotTempFile.getAbsolutePath(), type, file);
            
            if (dotTempFile.delete() == false) 
                System.out.println("Warning: " + dotTempFile.getAbsolutePath() + " could not be deleted!");
        } catch (Exception e) {
            System.out.println("Error while writing the dot source to a file!");
        }
    }
    
    /**
     * It will call the external dot program, and return the image in binary format.
     * @param DOTGraph Source of the graph (in dot language).
     * @param type Type of the output image to be produced, e.g.: gif, dot, fig, pdf, ps, svg, png.
     * @return The image of the graph in .gif format.
     */
    public static void convertDOTFileToImageFile(String DOTFile, String type, String imageFile){
        try {
        	File img = new File(imageFile);
            Runtime rt = Runtime.getRuntime();

            // patch by Mike Chenault
            String[] args = {DOT, "-T"+type, "-Gdpi="+DPISize, DOTFile, "-o", img.getAbsolutePath()};
            Process p = rt.exec(args);

            p.waitFor();
        }
        catch (java.io.IOException ioe) {
            System.out.println("Error:    in I/O processing of tempfile\n");
            System.out.println("       or in calling external command");
            ioe.printStackTrace();
        }
        catch (java.lang.InterruptedException ie) {
            System.out.println("Error: the execution of the external program was interrupted");
            ie.printStackTrace();
        }
    }
} // end of class GraphViz