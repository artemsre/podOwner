FROM python:3.8-slim
RUN mkdir /app
WORKDIR /app
COPY requirements.txt podOwnWatcher.py ./
RUN pip3 install -r requirements.txt
CMD ["python3", "podOwnWatcher.py"]
