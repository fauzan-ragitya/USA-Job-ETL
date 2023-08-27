FROM python:3.8
WORKDIR /code
ADD requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY main.py main.py
COPY destination_config.yml destination_config.yml
COPY modules/ modules/
COPY custom_transform/ custom_transform/
CMD ["python3", "main.py"]