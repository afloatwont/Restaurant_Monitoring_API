FROM python:3.13.2-slim

# container working dir
WORKDIR /usr/src/app 

# copy this root dir to workdir
COPY . . 

# install packages
RUN pip install --no-cache-dir -r requirements.txt

RUN python src/data_loader_script.py

EXPOSE 80

ENV NAME World

CMD ["python", "src/main.py"]

