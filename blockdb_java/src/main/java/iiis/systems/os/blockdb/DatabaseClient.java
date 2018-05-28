package iiis.systems.os.blockdb;

import java.util.concurrent.TimeUnit;  
import iiis.systems.os.blockdb.BlockDatabaseGrpc.BlockDatabaseBlockingStub;  
import io.grpc.ManagedChannel;  
import io.grpc.netty.NettyChannelBuilder; 
import org.json.JSONException;
import org.json.JSONObject;
import java.net.InetSocketAddress;

public class DatabaseClient {
    public static void main(String[] args) throws Exception {

    	JSONObject config = Util.readJsonFile("config.json");
        config = (JSONObject)config.get("1");
        String address = config.getString("ip");
        int port = Integer.parseInt(config.getString("port"));

    	ManagedChannel channel = NettyChannelBuilder.forAddress(new InetSocketAddress(address, port))
    							.usePlaintext(true)
    							.build();
    	BlockDatabaseBlockingStub stub = BlockDatabaseGrpc.newBlockingStub(channel); 


    	String type = args[0];
    	String userID, fromID, toID;
    	int value;
    	Object param;
    	GetResponse getResp;
    	BooleanResponse boolResp;
    	switch (type) {
    		case "GET":
    			userID = args[1];
    			param = GetRequest.newBuilder().setUserID(userID).build();
    			getResp = stub.get((GetRequest) param);  
				System.out.println("GET " + userID + " | RETURN " + getResp.getValue()); 
    			break;

    		case "PUT":
                userID = args[1];
                value = Integer.parseInt(args[2]);
    			param = Request.newBuilder().setUserID(userID).setValue(value).build();
    			boolResp = stub.put((Request) param);
				System.out.println("PUT " + userID + " " + value + " | RETURN success:" + boolResp.getSuccess()); 
                break;

    		case "DEPOSIT":
                userID = args[1];
                value = Integer.parseInt(args[2]);
    			param = Request.newBuilder().setUserID(userID).setValue(value).build();
    			boolResp = stub.deposit((Request) param);
				System.out.println("DEPOSIT " + userID + " " + value + " | RETURN success:" + boolResp.getSuccess()); 
                break;

    		case "WITHDRAW":
                userID = args[1];
                value = Integer.parseInt(args[2]);
    			param = Request.newBuilder().setUserID(userID).setValue(value).build();
    			boolResp = stub.withdraw((Request) param);
				System.out.println("WITHDRAW " + userID + " " + value + " | RETURN success:" + boolResp.getSuccess()); 
                break;

    		case "TRANSFER":
                fromID = args[1];
                toID = args[2];
                value = Integer.parseInt(args[3]);
    			param = TransferRequest.newBuilder().setFromID(fromID).setToID(toID).setValue(value).build();
    			boolResp = stub.transfer((TransferRequest) param);
				System.out.println("TRANSFER " + fromID + " " + toID + " " + value + " | RETURN success:" + boolResp.getSuccess()); 
    			break;

    		case "LOGLENGTH":
    			getResp = stub.logLength(null);  
				System.out.println("LOGLENGTH | RETURN " + getResp.getValue()); 
    			break;

    		default:
    			System.out.println("ERROR: UNKNOWN REQUEST TYPE!");
    	}
    	channel.shutdown().awaitTermination(1, TimeUnit.SECONDS);
    }
}

