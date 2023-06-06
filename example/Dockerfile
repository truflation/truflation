# docker build -t my_ingestor .
# docker run -p 8000:5000 -it my_ingestor

FROM python:3.10.6-slim
RUN mkdir /ingestor
ADD . /ingestor
WORKDIR /ingestor
# RUN pip install -r src/truflation.data.egg-info/requires.txt
# RUN pip install .

# Copy your setup.cfg file into the working directory.
COPY ../../setup.cfg .

# Install pip dependencies.
RUN pip install .

# CMD ["python", "examples/csv_example/example.py"]


