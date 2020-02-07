FROM python:latest

WORKDIR /usr/local/bin

RUN pip install boto
RUN pip install beautifulsoup4
RUN pip 
RUN pip install lxml
RUN pip install virtualenv
RUN pip install requests
RUN pip install pandas

ADD Problem2.py .
COPY configr.ini /data/configr.ini

CMD ["python", "./Problem2.py"]
