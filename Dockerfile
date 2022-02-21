FROM python:3.8
WORKDIR /usr/src/app/
COPY requirements.txt /usr/src/app/
RUN pip3 install -r /usr/src/app/requirements.txt
COPY . /usr/src/app/
CMD ["python", "main.py"]