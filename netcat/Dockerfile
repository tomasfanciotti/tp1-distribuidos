FROM python3

WORKDIR /app/
COPY ./netcat/test_server.py .
RUN chmod 111 test-server.sh
ENTRYPOINT ["/bin/sh"]