package iiis.systems.os.blockdb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import iiis.systems.os.blockdb.hash.Hash;

import org.json.JSONObject;
import org.json.JSONArray;

public class TBlock {
	String jsonString;
	JSONObject json, whole;
	String hash;
	HashMap<String, Integer> balances = new HashMap<>();

	TBlock prev;
	int height;

	TBlock(String jsonString, TBlock prev) {
		this.prev = prev;
		this.jsonString = jsonString;
		if (prev == null)
			this.height = 0;
		else
			this.height = this.prev.height + 1;
		this.json = new JSONObject(jsonString);
		this.whole = new JSONObject(jsonString);
		this.whole.put("jsonString", jsonString);
		this.hash = Hash.getHashString(jsonString);
		if (prev != null) {
			Iterator iter = this.prev.balances.entrySet().iterator();
			while(iter.hasNext()) {
			    Map.Entry entry = (Map.Entry)iter.next();
			    this.balances.put((String)entry.getKey(), (Integer)entry.getValue());
			}
		}
	}

	boolean applyTransaction(JSONObject transaction, String minerID) {
		String fromID = transaction.getString("FromID");
        String toID = transaction.getString("ToID");
        int value = transaction.getInt("Value");
        int miningFee = transaction.getInt("MiningFee");
        String uuid = transaction.getString("UUID");

    	int fromBalance = getOr1000(fromID);
    	int toBalance = getOr1000(toID);
        int minerBalance = getOr1000(minerID);
        if (fromBalance < value || value < miningFee || miningFee <= 0)
            return false;
    	balances.put(fromID, fromBalance - value);
    	balances.put(toID, toBalance + value - miningFee);
        balances.put(minerID, minerBalance + miningFee);

    	return true;
	}

	boolean applyAll() {
		JSONArray transactions = json.getJSONArray("Transactions");
		String minerID = json.getString("MinerID");
		for (Object transaction : transactions)
			if (!applyTransaction((JSONObject) transaction, minerID))
				return false;
		return true;
	}

	void update(String nonce) {
		json.put("Nonce", nonce);
		whole.put("Nonce", nonce);
		jsonString = json.toString();
		whole.put("jsonString", jsonString);
		hash = Hash.getHashString(jsonString);
	}

    int getOr1000(String userId) {
        if (balances.containsKey(userId)) {
            return balances.get(userId);
        } else {
            return 1000;
        }
    }

    TBlock getLCA(TBlock y)
    {
    	TBlock x = this;
    	while (x.height > y.height)
    		x = x.prev;
    	while (y.height > x.height)
    		y = y.prev;
    	while (x != y) {
    		x = x.prev;
    		y = y.prev;
    	}
    	return x;
    }
}
