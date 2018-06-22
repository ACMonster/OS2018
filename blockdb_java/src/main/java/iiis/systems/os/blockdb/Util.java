package iiis.systems.os.blockdb;

import org.json.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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

    public static boolean writeJsonFile(String fileName, JSONObject o) {
    	boolean success = true;
    	try {
            Path filePath = Paths.get(fileName);
            if (!Files.exists(filePath))
                Files.createFile(filePath);
    		Files.write(filePath, o.toString().getBytes());
    	} catch (IOException e) {
            System.out.println(e);
    		success = false;
    	}
    	return success;
    }
}
