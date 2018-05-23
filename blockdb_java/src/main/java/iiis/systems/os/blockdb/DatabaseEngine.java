package iiis.systems.os.blockdb;

import java.util.HashMap;

import org.json.JSONObject;
import org.json.JSONArray;

import java.io.IOException;

public class DatabaseEngine {
    private static DatabaseEngine instance = null;

    static final String logFileName = "log.txt";

    public static DatabaseEngine getInstance() {
        return instance;
    }

    public static void setup(String dataDir) {
        instance = new DatabaseEngine(dataDir);
    }

    private HashMap<String, Integer> balances = new HashMap<>();
    private int logLength = 0;
    private String dataDir;

    DatabaseEngine(String dataDir) {
        this.dataDir = dataDir;
        JSONObject logObject = Util.readJsonFile(dataDir + logFileName);
        if (logObject == null) {
        	logObject = new JSONObject();
        	logObject.put("numBlocks", 0);
        	logObject.put("Transactions", new JSONArray());
        	Util.writeJsonFile(dataDir + logFileName, logObject);
        }
    }

    private int getOrZero(String userId) {
        if (balances.containsKey(userId)) {
            return balances.get(userId);
        } else {
            return 0;
        }
    }

    private void addTransaction(int op, String userID, String fromID, String toID, int value) {
    	JSONObject transaction = new JSONObject();
    	switch (op) {
    		case putOp:
    			transaction.put("Type", "PUT");
    			transaction.put("UserID", userID);
    			transaction.put("Value", value);

    		case depositOp:
    			transaction.put("Type", "DEPOSIT");
    			transaction.put("UserID", userID);
    			transaction.put("Value", value);

    		case withdrawOp:
    			transaction.put("Type", "WITHDRAW");
    			transaction.put("UserID", userID);
    			transaction.put("Value", value);

    		case transferOp:
    			transaction.put("Type", "TRANSFER");
    			transaction.put("FromID", fromID);
    			transaction.put("ToID", toID);
    			transaction.put("Value", value);

    		default:
    			System.out.println("ERROR: UNKNOWN TRANSACTION TYPE!");
    	}
    	JSONObject logObject = Util.readJsonFile(dataDir + logFileName);
    	logObject.getJSONArray("Transactions").put(transaction);
    	Util.writeJsonFile(dataDir + logFileName, logObject);
    }

    public int get(String userId) {
        return getOrZero(userId);
    }

    public boolean put(String userId, int value) {
        logLength++;
        addTransaction(putOp, userId, null, null, value);
        balances.put(userId, value);
        return true;
    }

    public boolean deposit(String userId, int value) {
        logLength++;
        addTransaction(depositOp, userId, null, null, value);
        int balance = getOrZero(userId);
        balances.put(userId, balance + value);
        return true;
    }

    public boolean withdraw(String userId, int value) {
        logLength++;
        int balance = getOrZero(userId);
        addTransaction(withdrawOp, userId, null, null, value);
        balances.put(userId, balance - value);
        return true;
    }

    public boolean transfer(String fromId, String toId, int value) {
        logLength++;
        int fromBalance = getOrZero(fromId);
        int toBalance = getOrZero(toId);
        addTransaction(transferOp, null, fromId, toId, value);
        balances.put(fromId, fromBalance - value);
        balances.put(toId, toBalance + value);
        return true;
    }

    public int getLogLength() {
        return logLength;
    }

    private final int putOp = 1;
    private final int depositOp = 2;
    private final int withdrawOp = 3;
    private final int transferOp = 4;
}
