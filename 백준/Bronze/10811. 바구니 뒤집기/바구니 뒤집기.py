n,m=map(int,input().split(' '))
l=[_+1 for _ in range(n)]
for _ in range(m):
    a,b=map(int,input().split(' '))
    l[a-1:b]=l[b-1:a-2 if a>1 else None:-1]
print(' '.join(map(str,l)))