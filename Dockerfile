FROM python:3.8.9-slim-buster

ADD requirements.txt /tmp/requirements.txt
RUN pip3 install -r /tmp/requirements.txt
ADD scraper.py /root/scraper.py

CMD python3 /root/scraper.py
