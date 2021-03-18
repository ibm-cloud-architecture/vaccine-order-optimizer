FROM python:3.7.4-stretch
ENV PATH=/root/.local/bin:$PATH

ENV PYTHONPATH=/app

# RUN pip install --upgrade pip \
#   && pip install pipenv flask gunicorn

RUN pip install --upgrade pip \
  && pip install pipenv flask>=1.1.1 gunicorn flask-restful>=0.3.8 flasgger>=0.9.5 jsonschema pandas docplex==2.15.194 cplex==12.10.0.3 confluent-kafka>=1.4.0 avro-python3==1.10.0 cloudevents==1.2.0 fastavro==1.0.0.post1 prometheus_client requests

ADD . /app
WORKDIR /app
# First we get the dependencies for the stack itself
# RUN pipenv lock -r > requirements.txt
# RUN pip install -r requirements.txt
EXPOSE 5000
CMD ["gunicorn", "-w 1", "-b 0.0.0.0:5000", "app:app"]
