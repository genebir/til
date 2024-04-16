n,m=map(int,input().split())
d={}
for _ in input().split():
    d[_]=1
for _ in input().split():
    d[_]=d.get(_,0)+1
print(len([v for v in d.values() if v==1]))