# passivbot
trading bot running on binance isolated margin


requires python >= 3.8


dependencies, install with pip:


`python3.8 -m pip install matplotlib pandas websockets ccxt`


binance account needs margin enabled,

use at own risk

------------------------------------------------------------------

usage:

add api key and secret as json file in dir

api_key_secret/binance/your_user_name.json

formatted like this:

`["KEY", "SECRET"]`


if using non-default settings, make a copy of

settings/binance_isolated_margin/default.json

rename the copy your_user_name.json

and make changes

otherwise, the bot will use default settings

it will use the account's trade history to make trading decisions

if using an account with recent trade history and you wish to start fresh,
consider changing "snapshot_timestamp_millis" from 0 to current unix time in milliseconds, see more below

run in terminal:

`python3.8 passivbot_isolated_margin.py your_user_name`



------------------------------------------------------------------
overview

the bot's purpose is to accumulate btc

it simultaneously longs and shorts any btc quoted market

it longs by making small bids and shorts by borrowing coin from binance and making small asks

it listens to websocket stream and updates its orders continuously

it will look back in its own trade history and conglomerate all small buys into one big sell, and all small sells into one big buy

it exits longs by summing up all btc spent and all coin acquired on small long buys since previous full long exit,
placing a big long ask whose price is sum(btc_spent) / sum(coin_acquired) * (1 + 0.005),

the big long ask is adjusted after each new long buy

if it runs out of btc, it will borrow btc, and repay btc debt after long positions are filled

inversely,
it exits shorts by summing up all btc acquired and all coin spent on small short sells since previous full short exit,
placing a big short bid whose price is sum(btc_acquired) / sum(coin_spent) * (1 - 0.005)

when the short exit position is filled, it repays the coin debt

the big short bid is adjusted after each new short sell

by default it will make exits at at least 0.5% markup to cover fees to exchange and profit

net profit per exit will depend on vip level


interest paid on loans from exchange will also reduce profits

----------------------------------------------------------------------------------------

here follow example illustrations of behavior with ETH/BTC for two months

blue dots are small long buys, red dots are big long sells, red line is long sell prices

![long](/docs/ethbtc_long.png)



red dots are small short sells, blue dots are big short buys, blue line is short buy prices

![short](/docs/ethbtc_shrt.png/)







------------------------------------------------------------------
the size of its bids and asks scale with account equity

the market pairs in default settings are arbitrarily chosen, trading them is neither recommended nor discouraged


about the settings:

         "ETH/BTC": {
            "ema_spans_minutes": [15, 25, 40, 64, 102, 164, 263, 421, 675, 1080],   # no bid will be higher than min(emas), no ask will be lower than max(emas)
            "max_memory_span_days": 60,                                             # my_trades_age_limit = max(snapshot_timestamp_millis,
            "snapshot_timestamp_millis": 0,                                         #                           now - max_memory_span_millis)
            "min_markup_pct": 0.0025,                                               # long exit prices are at least 0.5%, max ~10%, higher than
            "max_markup_pct": 0.05,                                                 # long volume weighted average price, inverse with shorts
            "markup_pct_multiplier": 0.5,                                           # long_exit_price = long_vwap * (1 + min(max_markup_pct,
                                                                                    #                                        max(min_markup_pct, -bag_ratio)))
                                                                                    # shrt_exit_price = shrt_vwap * (1 - min(max_markup_pct,
                                                                                    #                                        max(min_markup_pct, bag_ratio)))
                                                                                    # where bag_ratio = ((long_cost - shrt_cost) / equity) * profit_pct_multiplier
            "entry_spread": 0.001,                                                  # max_bid_price = min(emas) * (1 - entry_spread / 2)
                                                                                    # min_ask_price = max(emas) * (1 + entry_spread / 2)
            "entry_vol_modifier_exponent": 20,                                      # entry volume is modified by the following formula:
                                                                                    # max_long_entry_vol *= max(
                                                                                    #     1.0,
                                                                                    #     min(min_exit_cost_multiplier / 2,
                                                                                    #        (long_exit_price / current_price)^entry_vol_modifier_exponent)
                                                                                    # )
                                                                                    # max_shrt_entry_vol *= max(
                                                                                    #     1.0,
                                                                                    #     min(min_exit_cost_multiplier / 2,
                                                                                    #        (current_price / shrt_exit_price)^entry_vol_modifier_exponent)
                                                                                    # )
                                                                                    # bigger difference between exit_price and current price gives bigger entries
                                                                                    # set entry_vol_modifier_exponent = 0 and there will be no
                                                                                    # entry_vol modification
            "min_exit_cost_multiplier": 20,                                         # exits are at least 10 times bigger than entries
            "long": true,
            "shrt": true,
            "account_equity_pct_per_hour": 0.001,
            "account_equity_pct_per_entry": 0.0001
        },




it will automatically place and delete orders, borrow and repay

it will only make orders, never (except by accident) take orders

it maintains up to 5 orders per market pair


one long entry: small bid

one long exit: big ask

one short entry: small ask

one short exit: big bid

one liquidation order in case of mismatch between balance and analysis of past trades

it will automatically analyze past trades and make appropriate long and short exits

-------------------------------------------------------------------------

if bot is max leveraged and cannot borrow more from exchange, it will borrow from self, keeping track of internal debt

