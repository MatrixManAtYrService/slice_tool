from collections import namedtuple

Interval = namedtuple("Interval", "start end")

# given a n and a list, return several lists less than n items long
# such that when they are concatentated, you get the original list back
def partition(num, data):
    return [data[x:x+num] for x in range(0, len(data), num)]
