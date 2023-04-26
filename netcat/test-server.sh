PORT=12345
if [ ! "$1" == "" ]; then
    PORT=$1
fi
nc -v server "$PORT"