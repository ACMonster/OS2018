package iiis.systems.os.blockdb;

import org.json.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Util {
    public static JSONObject readJsonFile(String filePath) {
    	try {
    		String content = new String(Files.readAllBytes(Paths.get(filePath)));
    		return new JSONObject(content);	
    	} catch (IOException e) {
    		return null;
    	}
    }

    public static boolean writeJsonFile(String filePath, JSONObject o) {
    	boolean success = true;
    	try {
    		Files.write(Paths.get(filePath), o.toString().getBytes());
    	} catch (IOException e) {
    		success = false;
    	}
    	return success;
    }
}
