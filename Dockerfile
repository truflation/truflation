# docker build -t my_ingestor .
# docker run -p 8000:5000 -it my_ingestor

FROM python:3.10.6-slim
RUN ["apt-get", "update"]
RUN ["apt-get", "install", "-y", "git"]
RUN mkdir /ingestor
ADD . /ingestor
WORKDIR /ingestor
RUN pip install . && python -mpytest .
