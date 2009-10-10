package BFT.serverShim;


import java.io.FilenameFilter;
import java.io.File;


public class LogFilter implements FilenameFilter{

    String id;
    public LogFilter(String id){
	this.id = id;
    }

    public boolean accept(File dir, String name){
	return name.endsWith(id);
    }

}