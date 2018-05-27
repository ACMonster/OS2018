package iiis.systems.os.blockdb;

import java.util.HashMap;

import org.json.JSONObject;
import org.json.JSONArray;

import java.io.IOException;

public class DatabaseEngine {
    private static DatabaseEngine instance = null;

    static final String logFileName = "log.txt";

    static final int blockSize = 50;

    private JSONObject transientLog;

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

        transientLog = Util.readJsonFile(dataDir + logFileName);
        if (transientLog == null) {
        	transientLog = new JSONObject();
        	transientLog.put("numBlocks", 0);
        	transientLog.put("Transactions", new JSONArray());
        	Util.writeJsonFile(dataDir + logFileName, transientLog);
        }

        int numBlocks = transientLog.getInt("numBlocks");
        JSONArray transactions = transientLog.getJSONArray("Transactions");
        logLength = transactions.length();

        for (int num = 1; num <= numBlocks; num++) {
        	JSONObject block = Util.readJsonFile(dataDir + num + ".json");
        	JSONArray blockTransactions = block.getJSONArray("Transactions");
	        for (Object transaction: blockTransactions)
	        	applyTransaction((JSONObject) transaction);
        }

        for (Object transaction: transactions)
        	applyTransaction((JSONObject) transaction);
    }

    private int getOrZero(String userId) {
        if (balances.containsKey(userId)) {
            return balances.get(userId);
        } else {
            return 0;
        }
    }

    private void applyTransaction(JSONObject transaction) {
    	String type = transaction.getString("Type");
        int value = transaction.getInt("Value");
    	String userID;
    	int balance;

    	switch (type) {
    		case "PUT":
                userID = transaction.getString("UserID");
    			balances.put(userID, value);
                break;

    		case "DEPOSIT":
                userID = transaction.getString("UserID");
		        balance = getOrZero(userID);
		        balances.put(userID, balance + value);
                break;

    		case "WITHDRAW":
                userID = transaction.getString("UserID");
		        balance = getOrZero(userID);
		        balances.put(userID, balance - value);
                break;

    		case "TRANSFER":
    			String fromID = transaction.getString("FromID");
    			String toID = transaction.getString("ToID");
    			int fromBalance = getOrZero(fromID);
    			int toBalance = getOrZero(toID);
    			balances.put(fromID, fromBalance - value);
    			balances.put(toID, toBalance + value);
                break;

    		default:
    			System.out.println("ERROR: UNKNOWN TRANSACTION TYPE!");
    	}
    }

    private void addTransaction(int op, String userID, String fromID, String toID, int value) {
    	JSONObject transaction = new JSONObject();
    	switch (op) {
    		case putOp:
    			transaction.put("Type", "PUT");
    			transaction.put("UserID", userID);
    			transaction.put("Value", value);
                break;

    		case depositOp:
    			transaction.put("Type", "DEPOSIT");
    			transaction.put("UserID", userID);
    			transaction.put("Value", value);
                break;

    		case withdrawOp:
    			transaction.put("Type", "WITHDRAW");
    			transaction.put("UserID", userID);
    			transaction.put("Value", value);
                break;

    		case transferOp:
    			transaction.put("Type", "TRANSFER");
    			transaction.put("FromID", fromID);
    			transaction.put("ToID", toID);
    			transaction.put("Value", value);
                break;

    		default:
    			System.out.println("ERROR: UNKNOWN TRANSACTION TYPE!");
    	}

    	transientLog.getJSONArray("Transactions").put(transaction);
    	logLength++;
    	if (logLength == blockSize) {
    		int num = transientLog.getInt("numBlocks") + 1;

    		JSONObject block = new JSONObject();
    		block.put("BlockID", num);
    		block.put("PrevHash", "00000000");
    		block.put("Transactions", transientLog.getJSONArray("Transactions"));
    		block.put("Nonce", "00000000");
    		Util.writeJsonFile(dataDir + num + ".json", block);

    		transientLog.put("numBlocks", num);
    		transientLog.put("Transactions", new JSONArray());
            logLength = 0;
    	}
        Util.writeJsonFile(dataDir + logFileName, transientLog);
    }

    public int get(String userId) {
        return getOrZero(userId);
    }

    public boolean put(String userId, int value) {
        if (value < 0)
            return false;
        addTransaction(putOp, userId, null, null, value);
        balances.put(userId, value);
        return true;
    }

    public boolean deposit(String userId, int value) {
        if (value < 0)
            return false;
        addTransaction(depositOp, userId, null, null, value);
        int balance = getOrZero(userId);
        balances.put(userId, balance + value);
        return true;
    }

    public boolean withdraw(String userId, int value) {
        if (value < 0)
            return false;
        int balance = getOrZero(userId);
        if (balance < value)
        	return false;
        addTransaction(withdrawOp, userId, null, null, value);
        balances.put(userId, balance - value);
        return true;
    }

    public boolean transfer(String fromId, String toId, int value) {
        if (value < 0)
            return false;
        int fromBalance = getOrZero(fromId);
        int toBalance = getOrZero(toId);
        if (fromBalance < value)
        	return false;
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
