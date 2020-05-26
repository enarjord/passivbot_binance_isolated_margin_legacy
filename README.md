# passivbot
trading bot running on binance margin


requires python >= 3.8


dependencies, install via pip:

pip3 install ccxt pandas numpy python-binance ciso8601


binance account needs margin enabled
works with both 3x and 5x

add api key secret to api_key_secret/binance/{user}.json:
["KEY", "SECRET"]

default settings are to trade these coins
ADA, ATOM, BAT, BCH, BNB, DASH, EOS, ETC, ETH, IOST,
IOTA, LINK, LTC, MATIC, NEO, ONT, QTUM, RVN, TRX, VET,
XLM, XMR, XRP, XTZ, ZEC
against BTC

the bot's goal is to accumulate BTC, both shorting and longing all coins simultaneously
it will automatically place and delete orders, borrow and repay



