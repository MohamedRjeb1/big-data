#!/usr/bin/env python3
"""
mapper.py
Hadoop streaming mapper: reads CSV from stdin, skips header, emits key: symbol|date and value: close,volume
Key and value separated by a tab for reducer.
"""
import sys

def mapper():
    first = True
    for line in sys.stdin:
        line=line.strip()
        if not line:
            continue
        if first:
            first = False
            if line.lower().startswith('timestamp'):
                continue
        parts = line.split(',')
        if len(parts) != 7:
            continue
        timestamp, open_p, high, low, close, volume, symbol = parts
        date = timestamp.split(' ')[0]
        key = f"{symbol}|{date}"
        print(f"{key}\t{close},{volume}")

if __name__ == '__main__':
    mapper()
