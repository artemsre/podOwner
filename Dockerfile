FROM python:3.8-slim
RUN mkdir /app
WORKDIR /app
COPY requirements.txt podOwnWatcher.py ./

