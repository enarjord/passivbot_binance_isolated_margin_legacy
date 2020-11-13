# passivbot
trading bot running on binance isolated margin

use at own risk


requires python >= 3.8


dependencies, install with pip:


`python3.8 -m pip install matplotlib pandas websockets ccxt`


------------------------------------------------------------------

released freely -- anybody may copy, redistribute, modify, use for commercial, non-commercial, educational or non-educational purposes, censor, denounce, or claim as one's own without permission from anybody

------------------------------------------------------------------

usage:

binance account needs isolated margin enabled for each symbol to trade

add api key and secret as json file in dir `api_key_secret/binance/your_user_name.json`

formatted like this: `["KEY", "SECRET"]`


if using non-default settings, make a copy of `settings/binance_isolated_margin/default.json`

rename the copy `your_user_name.json` and make changes

otherwise, the bot will use default settings

it will use the account's trade history to make trading decisions

if using an account with recent trade history and you wish to start fresh,
consider changing "snapshot_timestamp_millis" from 0 to current unix time in milliseconds, see more below

run in terminal: `python3.8 passivbot_isolated_margin.py your_user_name`


open `backtesting_notes_shared_equity.ipynb` with jupyter notebook for backtesting

------------------------------------------------------------------

overview

the bot's purpose is to accumulate btc

it can be used actively according to user's judgement of market conditions, but is designed and intended to work passively, i.e. "set and forget"

it simultaneously longs and shorts (optionally long only or short only) any btc quoted market

it favors neither direction, but allocates equal volume to shorts and longs

it longs by making small bids and shorts by borrowing coin from binance and making small asks

it listens to websocket stream and updates its orders continuously

it will look back in its own trade history to determine exit price and amount

it exits positions in both directions at volume weighted average entry price with a given markup

e.g.
long_exit_price = sum(long_entry_costs) / sum(long_entry_amounts) * (1 + markup)
shrt_exit_price = sum(shrt_entry_costs) / sum(shrt_entry_amounts) * (1 - markup)

after each new entry, the corresponding exit order's price and amount is updated

if it runs out of btc for long entries, it will borrow btc, repaying after long position is filled

inversely,
when the short exit position is filled, it repays the coin debt

----------------------------------------------------------------------------------------

here follow example illustrations of behavior

blue dots are small long buys, red dots are big long sells, red line is long entry vwap which resets after each exit

![long](/docs/xmr_btc_long.png)



red dots are small short sells, blue dots are big short buys, blue line is short entry vwap which resets after each exit

![short](/docs/xmr_btc_shrt.png/)







------------------------------------------------------------------
the size of its entries scale with account equity


about the settings:

        {
        "default": {
            "min_entry_delay_hours": 6.0,                     # min n hours between same side entries
            "ema_spans_minutes": [15, ... 1080],              # no bid is higher than min(emas),
                                                              # no ask is lower than max(emas)
            "entry_spread": 0.001,                            # no entry bid is higher than min(emas) * (1 - entry_spread)
                                                              # no entry ask is lower than max(emas) * (1 + entry_spread)
            "entry_vol_modifier_exponent": 10,                # entry size is multiplied by the following
            "min_exit_cost_multiplier": 10,                   # max(1.0, min(min_exit_cost_multiplier / 2,
                                                              #              vwap_last_price_ratio**entry_vol_modifier_exponent))
                                                              # where vwap_last_price_ratio = (long_vwap / last_price) or (last_price / shrt_vwap)
            "long": true,                                     # whether or not to long
            "shrt": true,                                     # whether or not to short
            "max_leverage": 10,                               # exchange's credit limit takes precendence
            "max_markup_pct": 0.1,                            # markup starts at 10%, declines to 0.5% over 18 days,
            "min_markup_pct": 0.005,                          # resets after each exit
            "n_days_to_min_markup": 18,                       # set n_days_to_min_markup = 0 for constant 0.5% markup
            "max_memory_span_days": 90,                       # my_trades older than 90 days are forgotten
            "snapshot_timestamp_millis": 0                    # my_trades older than snapshot are forgotten
            "phase_out": false,                               # if true, will stop new entries after exit
        },
        "global": {
            "max_entry_acc_val_pct_per_hour": 0.002           # max allowed percentage of total account value per side per hour
        },
        "symbols": {                                          # symbols to trade
            "AAA/BTC": {
                "long": true,
                "shrt": false,
                "max_leverage": 5                             # any value set here will override default setting
            },
            "BBB/BTC": {
                "min_markup_pct": 0.006,
                "snapshot_timestamp_millis": 1604421894519
            },
            "CCC/BTC": {
                "phase_out": true
            },
            "DDD/BTC": {},
            "EEE/BTC": {                                      # will liquidate any holdings
                "long": false,
                "shrt": false
            }

        },
        "user": "user_name"
    }




it will automatically place and delete orders, borrow and repay

it will only make orders, never (except by accident) take orders

it maintains up to 5 orders per market pair


one long entry: small bid

one long exit: big ask

one short entry: small ask

one short exit: big bid

one liquidation order in case of mismatch between balance and analysis of past trades

-------------------------------------------------------------------------
