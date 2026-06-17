FROM prefecthq/prefect:3-latest

WORKDIR /opt/prefect/app

COPY requirments.txt .

RUN pip install --no-cache-dir -r requirments.txt

RUN mkdir -p /opt/prefect/reports

COPY src/ ./src