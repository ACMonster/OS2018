package iiis.systems.os.blockdb;

import java.util.concurrent.TimeUnit;  
import iiis.systems.os.blockdb.BlockChainMinerGrpc.BlockChainMinerBlockingStub;  
import io.grpc.ManagedChannel;  
import io.grpc.netty.NettyChannelBuilder; 
import org.json.JSONException;
import org.json.JSONObject;
import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.List;
import java.util.ArrayList;
import java.util.Random;

public class BlockChainMinerClient {
    public static void main(String[] args) throws Exception {

    	JSONObject config = Util.readJsonFile("config.json"); 

        int numServers = config.getInt("nservers");
        int num = (new Random()).nextInt(numServers) + 1;
        JSONObject server = config.getJSONObject("" + num);
        ManagedChannel channel = NettyChannelBuilder.forAddress(new InetSocketAddress(server.getString("ip"), server.getInt("port")))
                                .usePlaintext(true)
                                .build();
        BlockChainMinerBlockingStub stub = BlockChainMinerGrpc.newBlockingStub(channel);

    	String type = args[0];
    	String userID, fromID, toID;
    	int value, miningFee;
    	String uuid, hash;
    	Object param;
    	GetResponse getResp;
    	BooleanResponse boolResp;
    	VerifyResponse verifyResp;
    	GetHeightResponse ghResp;
    	JsonBlockString jsonBS;

    	switch (type) {
    		case "GET":
    			userID = args[1];
    			param = GetRequest.newBuilder().setUserID(userID).build();
    			getResp = stub.get((GetRequest) param);  
				System.out.println("Server" + num + ": " + "GET " + userID + " | RETURN " + getResp.getValue()); 
    			break;


    		case "TRANSFER":
                fromID = args[1];
                toID = args[2];
                value = Integer.parseInt(args[3]);
                miningFee = Integer.parseInt(args[4]);
                if (args.length > 5)
                	uuid = args[5];
            	else
            		uuid = UUID.randomUUID().toString();
    			param = Transaction.newBuilder().setType(Transaction.Types.TRANSFER).setFromID(fromID).setToID(toID).setValue(value).setMiningFee(miningFee).setUUID(uuid).build();
                System.out.println("Server" + num + ": " + "TRANSFERING " + fromID + " " + toID + " " + value + " " + miningFee + " " + uuid); 
    			boolResp = stub.transfer((Transaction) param);
				System.out.println("Server" + num + ": " + "TRANSFER " + fromID + " " + toID + " " + value + " " + miningFee + " " + uuid + " | RETURN success:" + boolResp.getSuccess()); 
    			break;

    		case "VERIFY":
                fromID = args[1];
                toID = args[2];
                value = Integer.parseInt(args[3]);
                miningFee = Integer.parseInt(args[4]);
                if (args.length > 5)
                	uuid = args[5];
            	else
            		uuid = UUID.randomUUID().toString();
    			param = Transaction.newBuilder().setType(Transaction.Types.TRANSFER).setFromID(fromID).setToID(toID).setValue(value).setMiningFee(miningFee).setUUID(uuid).build();
    			verifyResp = stub.verify((Transaction) param);
    			System.out.println("Server" + num + ": " + "VERIFY " + fromID + " " + toID + " " + value + " " + miningFee + " " + uuid + " | RETURN result:" + verifyResp.getResult()); 
    			break;

    		case "GETHEIGHT":
    			ghResp = stub.getHeight(null);
    			System.out.println("Server" + num + ": " + "GETHEIGHT | RETURN result:" + ghResp.getHeight()); 
    			break;

    		case "GETBLOCK":
    			hash = args[1];
    			param = GetBlockRequest.newBuilder().setBlockHash(hash).build();
				jsonBS = stub.getBlock((GetBlockRequest) param);
				System.out.println("Server" + num + ": " + "GETBLOCK " + hash + " | RETURNS " + jsonBS.getJson());

    		default:
    			System.out.println("ERROR: UNKNOWN REQUEST TYPE!");
    	}

    	channel.shutdown().awaitTermination(1, TimeUnit.SECONDS);
    }
}

