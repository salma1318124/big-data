#!/usr/bin/env python3

import sys

# Read from stdin
for line in sys.stdin:
    # Remove leading/trailing whitespace
    line = line.strip()
    
    # Split the line into words
    words = line.split()
    
    # Output each word with count 1
    for word in words:
        # Remove punctuation and convert to lowercase
        word = word.lower().strip('.,!?;:\'\"')
        if word:
            print(f'{word}\t1')
