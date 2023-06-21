FROM python:3.10

ARG TELEGRAM_API_KEY
ENV TELEGRAM_API_KEY=$TELEGRAM_API_KEY
ARG TELEGRAM_CHAT_ID
ENV TELEGRAM_CHAT_ID=$TELEGRAM_CHAT_ID

RUN mkdir /app
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY rpchealth.py .

CMD ["python3", "rpchealth.py"]
