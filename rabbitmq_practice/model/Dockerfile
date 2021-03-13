FROM python:3.8
COPY ./src/ /var/usr/src/
COPY ./requirements.txt /var/usr/
WORKDIR /var/usr/src/
RUN pip install -r /var/usr/requirements.txt
CMD ["python", "model.py"]