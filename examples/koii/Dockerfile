# docker build -t koii-data .
# docker run -p 3001:3001 -it koii-data 
# docker run -it --rm koii-data /bin/bash
# docker build --no-cache -t koii-data .

# Use python:3 as the base image
FROM python:3

# Set the working directory in the container
WORKDIR /koii

# Create a directory and make it writable
RUN mkdir -p /koii/output && chmod -R 777 /koii/output

# Copy the rest of your application code to the working directory
COPY . .

# Install Python libraries
RUN pip install pandas

# Install Node.js
RUN apt-get update && apt-get install -y nodejs npm
RUN apt-get install -y nodejs

# Install Node.js dependencies
RUN npm install --no-scripts

# Expose the port your app runs on
EXPOSE 3001

# Command to run your application
CMD ["python", "src/subProcessInvocation.py"]
