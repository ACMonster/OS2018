#!/bin/sh
# Usage: ./start.sh --id=x
exec java -jar target/blockdb-1.0-SNAPSHOT.jar "$*"
