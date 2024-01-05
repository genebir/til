import sys;input=sys.stdin.readline
tmp=[0]*10001
for _ in range(int(input())):
    tmp[int(input())] += 1
for i,v in enumerate(tmp):
    for _ in range(v):
        print(i)