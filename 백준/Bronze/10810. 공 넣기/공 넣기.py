n,m=map(int,input().split(' '))
d=dict(zip(map(str,range(1,n+1)),[0 for _ in range(n)]))
for _ in range(m):
    a,b,c=map(int,input().split(' '))
    for _ in range(a,b+1):
        d[str(_)]=c
print(' '.join(map(str,d.values())))