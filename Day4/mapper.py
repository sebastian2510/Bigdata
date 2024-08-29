import sys

for line in sys.stdin:
    line = line.strip()
    print(f"chars\t{len(line)}")
    print(f"words\t{len(line.split())}")
    print("lines\t1")