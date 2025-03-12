# Restaurant Monitoring API

An API service to monitor restaurant uptime and downtime during business hours. Loop Assignment

## Setup and Installation

0. Save all the csv files in a directory named "data" in the root directory 

1. Install the required packages:

```bash
pip install -r requirements.txt
```

2. Initialize the sqlite database: 
```bash
python src/data_loader_script.py
```

3. Run the FastAPI app: 
```bash
python src/main.py
```

## Running the App with Docker

1. Build the Docker image:

```bash
docker build -t restaurant-monitoring-api .
```

2. Run the Docker container:

```bash
docker run -p 8000:80 restaurant-monitoring-api
```