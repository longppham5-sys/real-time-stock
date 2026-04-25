#!/usr/bin/env python
import sys

current_word = None
current_count = 0
word = None

# Input comes from stdin (the sorted output of the mapper)
for line in sys.stdin:
    line = line.strip()
    
    # Parse the input from mapper.py (word, 1)
    try:
        word, count = line.split('\t', 1)
        count = int(count)
    except ValueError:
        continue

    # Hadoop sorts map output by key (word) before passing it to the reducer
    if current_word == word:
        current_count += count
    else:
        if current_word:
            # Write result to stdout
            print("{0}\t{1}".format(current_word, current_count))
        current_word = word
        current_count = count

# Don't forget the last word!
if current_word == word:
    print("{0}\t{1}".format(current_word, current_count))