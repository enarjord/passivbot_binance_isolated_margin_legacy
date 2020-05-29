# passivbot
trading bot running on binance margin


requires python >= 3.8


dependencies, install with pip:


`python3.8 -m pip install ccxt python-binance`


binance account needs margin enabled,
works with both 3x and 5x margin

use at own risk

------------------------------------------------------------------

usage:

add api key and secret as json file in dir

api_key_secret/binance/your_user_name.json

formatted like this:

`["KEY", "SECRET"]`


if using non-default settings, make a copy of

settings/binance/default.json

rename the copy your_user_name.json

and make changes

otherwise, the bot will use default settings


run in terminal:

`python3.8 passivbot.py your_user_name`

------------------------------------------------------------------
overview

the bot's purpose is to accumulate btc

it simultaneously longs and shorts any btc quoted market

it longs by making small bids and shorts by borrowing coin from binance and making small asks

it exits longs by summing up all btc spent and all coin acquired on small long buys,
and places a big long ask whose price is sum(btc_spent) / sum(coin_acquired) * (1 + 0.0025)

if it runs out of btc, it will borrow btc, and repay btc debt after long positions are filled

inversely,
it exits shorts by summing up all btc acquired and all coin spent on small short sells,
and places a big short bid whose price is sum(btc_acquired) / sum(coin_spent) * (1 - 0.0025)

when the short exit position is filled, it repays the coin debt

here follow example illustrations of behavior with ETH/BTC for two months


blue dots are small long buys, red dots are big long sells, red line is long sell prices

![long](/docs/ethbtc_long.png)



red dots are small short sells, blue dots are big short buys, blue line is short buy prices

![short](/docs/ethbtc_shrt.png/)







------------------------------------------------------------------
the size of its bids and asks scale with account equity

it functions better with > 0.01 BTC to work with


about the settings:

    "quot": "BTC",                              # the coin to accumulate
    "coins_long": ["ADA", "ATOM", "BAT", ...]   # coins to long
                                                # default is all margin enabled coins which have BTC as quote
    "coins_shrt": ["ADA", "ATOM", "BAT", ...]   # coins to short
                                                # default is all margin enabled coins, except BNB, which have BTC as quote
                                                #
    "profit_pct": 0.0025,                       # minimum target profit per long/short exit
                                                # eg. if over time spent total 0.01 BTC buying long a total of 200.0 coin,
                                                # volume weighted average price is 0.01 / 200 == 0.00005
                                                # and long sell price becomes 0.00005 * (1 + 0.0025) == 0.00005125
                                                # inversely, if over time short sold a total of 200.0 coin for 0.01 BTC,
                                                # volume weighted average price is still 0.00005
                                                # and short buy price becomes 0.00005 * (1 - 0.0025) == 0.000049875
                                                #
    "account_equity_pct_per_trade": 0.0006,     # percentage of total account equity to spend per symbol per trade
    "account_equity_pct_per_hour": 0.0045,      # percentage of total account equity to spend per symbol per hour
    "hours_rolling_small_trade_window": 3.0,    # eg. if (past 3 hours long buy volume) > threshold: don't place long bid
    "bnb_buffer": 50.3,                         # BNB buffer for paying fees and interest, and for vip status
    "max_memory_span_days": 60,                 # how many days past the bot will take trades into account
    "ema_spans_minutes": [58, 70, ... 300, 360] # exponential moving averages used to set max bid and min ask prices
                                                # it calculates any number of emas,
                                                # and sets highest allowed bid = min(emas) and lowest allowed ask = max(emas)










by default it will long all coins and short all coins except BNB due to high interest rate

it will automatically place and delete orders, borrow and repay

it will only make orders, never (except by accident due to extreme volatility or exchange latency) take orders

it maintains up to 4 orders per market pair


one long entry: small bid

one long exit: big ask

one short entry: small ask

one short exit: big bid

it will automatically analyze past trades and make appropriate long and short exits

each market pair's volume is throttled by the rolling past 3 hours (default settings) same side volume:

if (past 3 hours long buy volume) > threshold: don't place long bid

if (past 3 hours short sell volume) > threshold: don't place short ask


if an exit is taken, will reset the correspondig side's timer

-------------------------------------------------------------------------

if bot is max leveraged and cannot borrow more from exchange, it will borrow from self, keeping track of internal debt

