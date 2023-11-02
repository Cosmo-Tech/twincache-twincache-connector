FROM python:3.10

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY twincache_connector.py twincache_connector.py
COPY main.py main.py
CMD python main.py
