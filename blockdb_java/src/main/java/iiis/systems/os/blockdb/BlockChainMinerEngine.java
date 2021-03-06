package iiis.systems.os.blockdb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import iiis.systems.os.blockdb.BlockChainMinerGrpc.BlockChainMinerBlockingStub;
import iiis.systems.os.blockdb.BlockChainMinerGrpc.BlockChainMinerStub;  

import java.util.ArrayList;  
import java.util.Collections;  
import java.util.Random;


import org.json.JSONObject;
import org.json.JSONArray;
import org.json.JSONException;
import iiis.systems.os.blockdb.hash.Hash;

import java.io.IOException;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.net.InetSocketAddress;
import io.grpc.ManagedChannel; 
import io.grpc.netty.NettyChannelBuilder; 
import io.grpc.stub.StreamObserver;
import java.lang.Throwable;

public class BlockChainMinerEngine {
    private static BlockChainMinerEngine instance = null;

    static final String logFileName = "log.txt";

    static final int blockSize = 50;

    private JSONObject transientLog;

    synchronized public static BlockChainMinerEngine getInstance() {
        return instance;
    }

    synchronized public static BlockChainMinerEngine setup(String dataDir, String name, List<InetSocketAddress> address) {
        instance = new BlockChainMinerEngine(dataDir, name, address);
        return instance;
    }

    private HashMap<String, Integer> balances = new HashMap<>();
    private HashMap<String, TBlock> blockHash = new HashMap<>();
    private HashMap<String, TBlock> transactionStatus = new HashMap<>();
    private List<TBlock> blocks = new ArrayList<>();
    private TBlock leaf, root, longest;
    private TBlock mining;
    private String dataDir, name;
    private List<BlockChainMinerBlockingStub> blockingStubs;
    private List<BlockChainMinerStub> stubs;
    private List<InetSocketAddress> address;

    BlockChainMinerEngine(String dataDir, String name, List<InetSocketAddress> address) {
        this.dataDir = dataDir;
        this.address = address;
        this.name = name;

        try {
            Files.createDirectories(Paths.get(dataDir));
        } catch (IOException e) {
            System.out.println(e);
        }

        root = new TBlock("{\"PrevHash\":\"0000000000000000000000000000000000000000000000000000000000000000\",\"Transactions\":[]}", null);
        root.hash = "0000000000000000000000000000000000000000000000000000000000000000";
        root.height = 0;
        blockHash.put("0000000000000000000000000000000000000000000000000000000000000000", root);
        blocks.add(root);
        leaf = longest = root;

        transientLog = Util.readJsonFile(dataDir + logFileName);
        if (transientLog == null) {
        	transientLog = new JSONObject();
        	transientLog.put("numBlocks", 0);
        	transientLog.put("Transactions", new JSONArray());
        	Util.writeJsonFile(dataDir + logFileName, transientLog);
        }

        int numBlocks = transientLog.getInt("numBlocks");

        for (int num = 1; num <= numBlocks; num++) {
        	JSONObject block = Util.readJsonFile(dataDir + num + ".json");
        	String jsonString = block.getString("jsonString");
	        pushBlock(jsonString);
        }

        JSONArray transactions = transientLog.getJSONArray("Transactions");
        for (Object transaction: transactions) 
            transactionStatus.put(((JSONObject)transaction).getString("UUID"), null);

        update();
    }

    synchronized private void createStubs() {
        stubs = new ArrayList<>();
        for (InetSocketAddress ad : address) {
            ManagedChannel channel = NettyChannelBuilder.forAddress(ad)
                                    .usePlaintext(true)
                                    .build();
            stubs.add(BlockChainMinerGrpc.newStub(channel));
        }
    }

    synchronized private void createBlockingStubs() {
        blockingStubs = new ArrayList<>();
        for (InetSocketAddress ad : address) {
            ManagedChannel channel = NettyChannelBuilder.forAddress(ad)
                                    .usePlaintext(true)
                                    .build();
            blockingStubs.add(BlockChainMinerGrpc.newBlockingStub(channel));
        }
    }

    synchronized private void update() {
        TBlock lca = leaf.getLCA(longest);

        int numBlocks = transientLog.getInt("numBlocks");

        for (int num = numBlocks + 1; num < blocks.size(); ++ num)
            Util.writeJsonFile(dataDir + num + ".json", blocks.get(num).whole);

        List<JSONArray> history = new ArrayList<>();
        history.add(transientLog.getJSONArray("Transactions"));
        for (TBlock block = leaf; block != lca; block = block.prev) {
            JSONArray transactions = block.json.getJSONArray("Transactions");
            history.add(transactions);
            for (Object transaction: transactions) 
                transactionStatus.put(((JSONObject)transaction).getString("UUID"), null);
        }
        for (TBlock block = longest; block != lca; block = block.prev) {
            JSONArray transactions = block.json.getJSONArray("Transactions");
            for (Object transaction: transactions) 
                transactionStatus.put(((JSONObject)transaction).getString("UUID"), block);
        }

        Collections.reverse(history);
        JSONArray merged = new JSONArray();
        for (JSONArray transactions : history) 
            for (Object transaction: transactions) {
                String uuid = ((JSONObject)transaction).getString("UUID");
                if (transactionStatus.containsKey(uuid) && transactionStatus.get(uuid) == null)
                    merged.put(transaction);
            }

        leaf = longest;
        transientLog.put("numBlocks", blocks.size() - 1);
        transientLog.put("Transactions", merged);
        Util.writeJsonFile(dataDir + logFileName, transientLog);

        mining = new TBlock("{\"BlockID\":" + blocks.size() + ",\"PrevHash\":\"" + leaf.hash + "\",\"Transactions\":[], \"MinerID\":\"" + name + "\"}", leaf);
        balances.clear();
        if (leaf != null) {
            Iterator iter = leaf.balances.entrySet().iterator();
            while(iter.hasNext()) {
                Map.Entry entry = (Map.Entry)iter.next();
                balances.put((String)entry.getKey(), (Integer)entry.getValue());
            }
        }

        for (Object transaction: merged) 
            applyTransaction((JSONObject) transaction, name);
    }

    int getOr1000(String userId) {
        if (balances.containsKey(userId)) {
            return balances.get(userId);
        } else {
            return 1000;
        }
    }

    synchronized private boolean applyTransaction(JSONObject transaction, String minerID) {

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
        
        if (mining.json.getJSONArray("Transactions").length() < blockSize) {
            mining.applyTransaction(transaction, minerID);

            mining.json.getJSONArray("Transactions").put(transaction);
            mining.whole.getJSONArray("Transactions").put(transaction);
        }

        return true;
    }


    synchronized public int get(String userId) {
        if (leaf != longest)
            update();
        return leaf.getOr1000(userId);
    }

    synchronized public boolean transfer(String fromID, String toID, int value, int miningFee, String uuid, boolean direct) {
        //System.out.println("Starting transfer: " + name + direct);
        if (leaf != longest)
            update();
        JSONObject transaction = new JSONObject();
        transaction.put("Type", "TRANSFER");
        transaction.put("FromID", fromID);
        transaction.put("ToID", toID);
        transaction.put("Value", value);
        transaction.put("MiningFee", miningFee);
        transaction.put("UUID", uuid);
        if (transactionStatus.containsKey(uuid))
            return false;
        //System.out.println("Doing transfer: " + name + direct);
        if (applyTransaction(transaction, name)) {
            transientLog.getJSONArray("Transactions").put(transaction);
            if (transientLog.getJSONArray("Transactions").length() % 1 == 0)
                Util.writeJsonFile(dataDir + logFileName, transientLog);

            transactionStatus.put(uuid, null);
            if (direct) {
                //System.out.println("Success transfer: " + name);
                Transaction request = Transaction.newBuilder().setFromID(fromID).setToID(toID).setValue(value).setMiningFee(miningFee).setUUID(uuid).build();
                createBlockingStubs();
                for (BlockChainMinerBlockingStub stub : blockingStubs)
                    stub.pushTransaction(request);
                //System.out.println("Success transfer again: " + name);
            }
            return true;
        }
        else
            return false;
    }

    synchronized public boolean transfer(String fromID, String toID, int value, int miningFee, String uuid) {
        return transfer(fromID, toID, value, miningFee, uuid, true);
    }

    synchronized public VerifyResponse.Results verify(String fromID, String toID, int value, int miningFee, String uuid, String hash) {
        if (leaf != longest)
            update();
        if (!transactionStatus.containsKey(uuid))
            return VerifyResponse.Results.FAILED;
        TBlock block = transactionStatus.get(uuid);
        if (block == null)
            return VerifyResponse.Results.PENDING;
        hash = block.hash;
        if (block.height + 6 <= leaf.height)
            return VerifyResponse.Results.SUCCEEDED;
        return VerifyResponse.Results.PENDING;
    }

    synchronized public int getHeight(String hash) {
        if (leaf != longest)
            update();
        hash = leaf.hash;
        return leaf.height;
    }

    synchronized public String getBlock(String hash) {
        if (leaf != longest)
            update();
        if (blockHash.containsKey(hash))
            return blockHash.get(hash).jsonString;
        return null;
    }

    synchronized public void pushBlock(String jsonString) {
        if (leaf != longest)
            update();
        Set hs = new HashSet();
        List<String> pending = new ArrayList<>();
        
        // System.out.println(name + " recevieves a pushBlock request: " + jsonString);
        String prevHash;
        try {
            JSONObject newJSON = new JSONObject(jsonString);
            if (newJSON.getJSONArray("Transactions").length() == 0)
                return;
            prevHash = newJSON.getString("PrevHash");
        } catch (JSONException e) {
            return;
        }

        while (true) {
            // System.out.println(name + " checking a block : with PrevHash = " +  prevHash);
            if (!Hash.checkHash(Hash.getHashString(jsonString)))
                return;
            pending.add(jsonString);
            if (blockHash.containsKey(prevHash))
                break;
            if (hs.contains(prevHash))
                return;
            hs.add(prevHash);
            jsonString = null;
            GetBlockRequest request = GetBlockRequest.newBuilder().setBlockHash(prevHash).build();
            createBlockingStubs();
            for (BlockChainMinerBlockingStub stub : blockingStubs) {
                jsonString = stub.getBlock(request).getJson();
                if (jsonString != null)
                    break;
            }
            if (jsonString == null)
                return;
        }
        
        Collections.reverse(pending);
        for (String json : pending) {
            try {
                prevHash = (new JSONObject(json)).getString("PrevHash");
            } catch (JSONException e) {
                return;
            }
            // System.out.println(name + " recevieves a block : with PrevHash = " +  prevHash);
            TBlock block = new TBlock(json, blockHash.get(prevHash));
            if (block.applyAll()) {
                // System.out.println(name + " accepts a block : with PrevHash = " +  prevHash);
                // haven't check "the block’s transactions are new transactions"
                blocks.add(block);
                if (block.height > longest.height)
                    longest = block;
                else if (block.height == longest.height && block.hash.compareTo(longest.hash) < 0)
                    longest = block;
                blockHash.put(block.hash, block);
            }
            else
                break;
        }
        // if (leaf != longest)
        //     update();
    }

    synchronized public void pushTransaction(String fromID, String toID, int value, int miningFee, String uuid) {
        // System.out.println(name + " Starting pushTransaction: " + name);
        transfer(fromID, toID, value, miningFee, uuid, false);
        // System.out.println(name + " Finishing pushTransaction: " + name);
    }


    synchronized public boolean compute(int iter) {
        if (leaf != longest)
            update();
        if (mining.json.getJSONArray("Transactions").length() == 0)
            return false;
        Random rand = new Random();
        String nonce;
        for (int i = 0; i < iter; ++ i) {
            nonce = "";
            nonce += rand.nextInt(10);
            nonce += rand.nextInt(10);
            nonce += rand.nextInt(10);
            nonce += rand.nextInt(10);
            nonce += rand.nextInt(10);
            nonce += rand.nextInt(10);
            nonce += rand.nextInt(10);
            nonce += rand.nextInt(10);
            mining.update(nonce);
            if (Hash.checkHash(mining.hash)) {
                pushBlock(mining.jsonString);
                JsonBlockString request = JsonBlockString.newBuilder().setJson(mining.jsonString).build();
                createStubs();
                for (BlockChainMinerStub stub : stubs) {
                    // System.out.println(name + " sneds a pushBlock request");
                    stub.pushBlock(request, new StreamObserver<Null>() {
                        @Override
                        public void onNext(Null x) {
                        }
                        @Override
                        public void onError(Throwable t) {
                        }
                        @Override
                        public void onCompleted() {
                        }
                    });
                }
                return true;
            }
        }
        return false;
    }

}
