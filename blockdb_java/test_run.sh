#!/bin/sh

# This script starts the database server and runs a series of operation against server implementation.
# If the server is implemented correctly, the output (both return values and JSON block) will match the expected outcome.
# Note that this script does not compare the output value, nor does it compare the JSON file with the example JSON.

# Please start this script in a clean environment; i.e. the server is not running, and the data dir is empty.

if [ ! -f "config.json" ]; then
	echo "config.json not in working directory. Trying to go to parent directory..."
	cd ../
fi
if [ ! -f "config.json" ]; then
  	echo "!! Error: config.json not found. Please run this script from the project root directory (e.g. ./example/test_run.sh)."
	exit -1
fi

echo "Testrun starting..."

./start.sh --id=1 &
PID1=$!
sleep 1

./start.sh --id=2 &
PID2=$!
sleep 1

./start.sh --id=3 &
PID3=$!
sleep 1

echo "Step 1: Quickly push many transactions"
for I in `seq 0 9`; do
	# param: from, to, value, fee
	java -cp target/blockdb-1.0-SNAPSHOT.jar iiis.systems.os.blockdb.BlockChainMinerClient TRANSFER USER000$I USER0099 5 1
done
sleep 10
echo "Check value: expecting value=995"
java -cp target/blockdb-1.0-SNAPSHOT.jar iiis.systems.os.blockdb.BlockChainMinerClient GET USER0005

echo "Step 2: Slowly push many transactions, should cause more blocks to be produced"
for I in `seq 0 9`; do
	java -cp target/blockdb-1.0-SNAPSHOT.jar iiis.systems.os.blockdb.BlockChainMinerClient TRANSFER USER000$I USER0099 5 1
	sleep 2
done
echo "You should already see 5~10 blocks."
sleep 10
echo "Check value: expecting value=1080"
java -cp target/blockdb-1.0-SNAPSHOT.jar iiis.systems.os.blockdb.BlockChainMinerClient GET USER0099

echo "Test completed. Please verify BlockChain is legitimate and all earliest transactions are verified."

kill $PID1
kill $PID2
kill $PID3
