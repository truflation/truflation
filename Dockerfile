# docker build -t my_ingestor .
# docker run -p 8000:5000 -it my_ingestor

FROM python:3.10.6-slim
RUN mkdir /ingestor
ADD . /ingestor
WORKDIR /ingestor
RUN pip install .
