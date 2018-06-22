package iiis.systems.os.blockdb;

import io.grpc.Server;
import iiis.systems.os.blockdb.BlockChainMinerGrpc.BlockChainMinerBlockingStub;  
import io.grpc.ManagedChannel; 
import io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.NettyChannelBuilder; 
import io.grpc.stub.StreamObserver;
import org.json.JSONException;
import org.json.JSONObject;
import java.util.ArrayList;  
import java.util.List;

import java.io.IOException;
import java.net.InetSocketAddress;

public class BlockChainMinerServer {
    private Server server;

    private void start(String address, int port, BlockChainMinerEngine engine) throws IOException {
        BlockChainMinerImpl impl = new BlockChainMinerImpl();
        impl.setEngine(engine);
        server = NettyServerBuilder.forAddress(new InetSocketAddress(address, port))
                .addService(impl)
                .build()
                .start();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                BlockChainMinerServer.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public static void main(String[] args) throws IOException, JSONException, InterruptedException {
        String id;
        if (args[0].startsWith("-id="))
            id = args[0].substring(4);
        else if (args[0].startsWith("--id="))
            id = args[0].substring(5);
        else {
            System.out.println("Usage: ./start.sh --id=x");
            return;
        }

        JSONObject config = Util.readJsonFile("config.json");
        int numServers = config.getInt("nservers");

        int numericID = Integer.parseInt(id);
        if (numericID < 1 || numericID > numServers) {
            System.out.println("Server ID should be between 1 and " + numServers);
            return;
        }

        List<BlockChainMinerBlockingStub> stubs = new ArrayList<>();
        for (int num = 1; num <= numServers; num++) 
            if (num != numericID) {
                JSONObject server = (JSONObject)config.get("" + num);
                ManagedChannel channel = NettyChannelBuilder.forAddress(new InetSocketAddress(server.getString("ip"), server.getInt("port")))
                                        .usePlaintext(true)
                                        .build();
                stubs.add(BlockChainMinerGrpc.newBlockingStub(channel));
            }

        config = (JSONObject)config.get(id);
        String address = config.getString("ip");
        int port = Integer.parseInt(config.getString("port"));
        String dataDir = config.getString("dataDir");

        String serverName = "Server";
        if (id.length() == 1)
            serverName += "0";
        serverName += id;
        final BlockChainMinerEngine engine = BlockChainMinerEngine.setup(dataDir, serverName, stubs);

        new Thread(new Runnable() {
            @Override
            public void run() {
                int total = 0;
                while (true) {
                    total += 100;
                    //if (total % 100000 == 0)
                    //    System.out.println("iteration" + total);
                    engine.compute(100);
                }
            }
        }).start();

        final BlockChainMinerServer server = new BlockChainMinerServer();
        server.start(address, port, engine);
        server.blockUntilShutdown();
    }

    static class BlockChainMinerImpl extends BlockChainMinerGrpc.BlockChainMinerImplBase {
        private BlockChainMinerEngine dbEngine;

        public void setEngine(BlockChainMinerEngine engine) {
            dbEngine = engine;
        }

        @Override
        public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
            int value = dbEngine.get(request.getUserID());
            GetResponse response = GetResponse.newBuilder().setValue(value).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void transfer(Transaction request, StreamObserver<BooleanResponse> responseObserver) {
            boolean success = dbEngine.transfer(request.getFromID(), request.getToID(), request.getValue(), request.getMiningFee(), request.getUUID());
            BooleanResponse response = BooleanResponse.newBuilder().setSuccess(success).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void verify(Transaction request, StreamObserver<VerifyResponse> responseObserver) {
            String hash = "";
            VerifyResponse.Results result = dbEngine.verify(request.getFromID(), request.getToID(), request.getValue(), request.getMiningFee(), request.getUUID(), hash);
            VerifyResponse response = VerifyResponse.newBuilder().setResult(result).setBlockHash(hash).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void getHeight(Null request, StreamObserver<GetHeightResponse> responseObserver) {
            String hash = "";
            int height = dbEngine.getHeight(hash);
            GetHeightResponse response = GetHeightResponse.newBuilder().setHeight(height).setLeafHash(hash).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void getBlock(GetBlockRequest request, StreamObserver<JsonBlockString> responseObserver) {
            String json = dbEngine.getBlock(request.getBlockHash());
            JsonBlockString response = JsonBlockString.newBuilder().setJson(json).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }


        @Override
        public void pushBlock(JsonBlockString request, StreamObserver<Null> responseObserver) {
            dbEngine.pushBlock(request.getJson());
            responseObserver.onNext(null);
            responseObserver.onCompleted();
        }


        @Override
        public void pushTransaction(Transaction request, StreamObserver<Null> responseObserver) {
            dbEngine.pushTransaction(request.getFromID(), request.getToID(), request.getValue(), request.getMiningFee(), request.getUUID());
            responseObserver.onNext(null);
            responseObserver.onCompleted();
        }



    }
}
