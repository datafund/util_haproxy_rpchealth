FROM python:3.10

RUN mkdir /app
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY rpchealth.py .

CMD ["gunicorn", "-w", "1", "rpchealth:app", "-b", "0.0.0.0:9999"]
