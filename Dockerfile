# Use the official Python image from the Docker Hub
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any necessary dependencies specified in requirements.txt
# If your project doesn't have a requirements.txt file, you can skip this step
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the project files into the container
COPY . .

RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. ./proto/raft.proto
# Command to run the application
# This can be overridden when running the container, e.g.,
# docker run <image_name> python3 raft_node.py node1
CMD ["python3", "raft_node.py", "node1"]

