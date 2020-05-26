# passivbot
trading bot running on binance margin


requires python >= 3.8


dependencies, install via pip:

pip3 install ccxt pandas numpy python-binance ciso8601


binance account needs margin enabled
works with both 3x and 5x margin


usage:
add api key secret to api_key_secret/binance/{user}.json:
["KEY", "SECRET"]

run in terminal:
passivbop.py {user}

------------------------------------------------------------------

default settings are to trade these coins
ADA, ATOM, BAT, BCH, BNB, DASH, EOS, ETC, ETH, IOST,
IOTA, LINK, LTC, MATIC, NEO, ONT, QTUM, RVN, TRX, VET,
XLM, XMR, XRP, XTZ, ZEC
against BTC

the bot's goal is to accumulate BTC, both shorting and longing all coins simultaneously
it is designed to benefit regardless of price moving up or down
it will automatically place and delete orders, borrow and repay

it will only make orders, never (except by accident due to extreme volatility or exchange latency) take orders
it maintains up to 4 orders per market pair

one long entry: small bid
one long exit: big ask
one short entry: small ask
one short exit: big bid

price of long exit is sum(all long entries cost) / sum(all long entries amount) * 1.0025 (default settings)
price of short exit is inversely sum(all short entries cost) / sum(all short entries amount) * 0.9975 (default settings)

it will automatically analyze past trades and make appropriate long and short exits

each market pair's volume is throttled by the rolling past 3 hours (default settings) same side volume:
if (past 3 hours long buy volume) > threshold: don't place long bid
if (past 3 hours short sell volume) > threshold: don't place short ask

if an exit is taken, will reset the correspondig side's timer

-------------------------------------------------------------------------

if bot is max leveraged and cannot borrow more from exchange, it will borrow from self, keeping track of internal debt

