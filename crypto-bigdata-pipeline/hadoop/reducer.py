#!/usr/bin/env python3
"""
reducer.py
Hadoop streaming reducer: reads key\tvalue where value is close,volume and aggregates for each key
Emits CSV: symbol,date,avg_price,total_volume,max_price,min_price,count
"""
import sys

def reducer():
    current_key = None
    count = 0
    sum_price = 0.0
    sum_volume = 0.0
    max_price = None
    min_price = None

    for line in sys.stdin:
        line=line.strip()
        if not line:
            continue
        try:
            key, val = line.split('\t',1)
            close_s, volume_s = val.split(',')
            close = float(close_s)
            volume = float(volume_s)
        except Exception:
            continue
        if current_key is None:
            current_key = key
        if key != current_key:
            # flush previous
            symbol, date = current_key.split('|')
            avg_price = sum_price / count if count else 0.0
            print(f"{symbol},{date},{avg_price},{sum_volume},{max_price},{min_price},{count}")
            # reset
            current_key = key
            count = 0
            sum_price = 0.0
            sum_volume = 0.0
            max_price = None
            min_price = None
        # aggregate
        count += 1
        sum_price += close
        sum_volume += volume
        if max_price is None or close > max_price:
            max_price = close
        if min_price is None or close < min_price:
            min_price = close

    if current_key is not None and count>0:
        symbol, date = current_key.split('|')
        avg_price = sum_price / count if count else 0.0
        print(f"{symbol},{date},{avg_price},{sum_volume},{max_price},{min_price},{count}")

if __name__ == '__main__':
    reducer()
