# passivbot
trading bot running on binance margin


requires python >= 3.8


dependencies, install with pip:


`python3.8 -m pip install ccxt pandas python-binance ciso8601`


binance account needs margin enabled,
works with both 3x and 5x margin

------------------------------------------------------------------

usage:

add api key and secret as json file in dir

api_key_secret/binance/your_user_name.json

formatted like this:
["KEY", "SECRET"]


if using non-default settings, make a copy of

settings/binance/default.json

rename the copy your_user_name.json

and make changes

otherwise, the bot will use default settings


run in terminal:

`passivbop.py your_user_name`

------------------------------------------------------------------

about the settings:

    "quot": "BTC",                              # the coin to accumulate
    "coins": ["ADA", "ATOM", "BAT", ...]        # coins to long/short. default is all margin enabled coins
    "profit_pct": 0.0025,                       # minimum target profit per long/short exit
                                                #
                                                # eg. if it over time spent total 0.01 BTC buying long a total of 200 coins,
                                                # volume weighted average price is 0.01 / 200 == 0.00005
                                                # and long sell price becomes 0.00005 * 1.0025 (0.25%) == 0.00005125
                                                #
    "account_equity_pct_per_trade": 0.0006,     # percentage of total account equity to spend per symbol per trade
    "account_equity_pct_per_hour": 0.0045,      # percentage of total account equity to spend per symbol per hour
    "hours_rolling_small_trade_window": 3.0,    # eg. if (past 3 hours long buy volume) > threshold: don't place long bid
    "bnb_buffer": 50.3,                         # BNB buffer for paying fees, interest and vip status
    "max_memory_span_days": 120,                # how many days past the bot will take trades into account
    "ema_spans_minutes": [58, 70, ... 300, 360] # exponential moving averages used to set max/min bid/ask price
                                                # it calculates any number of emas,
                                                # and sets highest allowed bid = min(emas) and lowest allowed ask = max(emas)










it will long all coins and short all coins except BNB due to high interest rate

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

