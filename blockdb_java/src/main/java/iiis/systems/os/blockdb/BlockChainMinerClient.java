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

public class BlockChainMinerClient {
    public static void main(String[] args) throws Exception {

    	JSONObject config = Util.readJsonFile("config.json"); 

        int numServers = config.getInt("nservers");
        List<ManagedChannel> channels = new ArrayList<>();
        List<BlockChainMinerBlockingStub> stubs = new ArrayList<>();
        for (int num = 1; num <= numServers; num++) {
            JSONObject server = (JSONObject)config.get("" + num);
            ManagedChannel channel = NettyChannelBuilder.forAddress(new InetSocketAddress(server.getString("ip"), server.getInt("port")))
                                    .usePlaintext(true)
                                    .build();
            channels.add(channel);
            stubs.add(BlockChainMinerGrpc.newBlockingStub(channel));
        }

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
    			getResp = stubs.get(0).get((GetRequest) param);  
				System.out.println("GET " + userID + " | RETURN " + getResp.getValue()); 
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
                System.out.println("TRANSFERING " + fromID + " " + toID + " " + value + " " + miningFee + " " + uuid); 
    			boolResp = stubs.get(0).transfer((Transaction) param);
				System.out.println("TRANSFER " + fromID + " " + toID + " " + value + " " + miningFee + " " + uuid + " | RETURN success:" + boolResp.getSuccess()); 
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
    			verifyResp = stubs.get(0).verify((Transaction) param);
    			System.out.println("VERIFY " + fromID + " " + toID + " " + value + " " + miningFee + " " + uuid + " | RETURN result:" + verifyResp.getResult()); 
    			break;

    		case "GETHEIGHT":
    			ghResp = stubs.get(0).getHeight(null);
    			System.out.println("GETHEIGHT | RETURN result:" + ghResp.getHeight()); 
    			break;

    		case "GETBLOCK":
    			hash = args[1];
    			param = GetBlockRequest.newBuilder().setBlockHash(hash).build();
				jsonBS = stubs.get(0).getBlock((GetBlockRequest) param);
				System.out.println("GETBLOCK " + hash + " | RETURNS " + jsonBS.getJson() );


    		default:
    			System.out.println("ERROR: UNKNOWN REQUEST TYPE!");
    	}
    	for (ManagedChannel channel : channels)
    		channel.shutdown().awaitTermination(1, TimeUnit.SECONDS);
    }
}

