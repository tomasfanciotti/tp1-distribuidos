FROM python:3.9.7-slim
WORKDIR /app/
COPY server .
# RUN python -m unittest tests/*
RUN pip install pika
ENTRYPOINT ["/bin/sh"]
