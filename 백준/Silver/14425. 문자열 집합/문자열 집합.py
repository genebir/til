n,m=map(lambda x: int(x), input().split());cnt=0
s=[input() for _ in range(n)]
for _ in range(m):
    cnt += int(input() in s)
print(cnt)