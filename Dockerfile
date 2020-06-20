FROM python:3.8
COPY . /app
WORKDIR /app
RUN python -m pip install ccxt python-binance
CMD ["python", "passivbot.py", "example_user"]
