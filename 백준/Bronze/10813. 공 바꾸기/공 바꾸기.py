n,m=map(int,input().split(' '))
n_=[_ for _ in range(1,n+1)]
for _ in range(m):
    s,t=map(int,input().split(' '))
    tmp=n_[s-1]
    n_[s-1]=n_[t-1]
    n_[t-1]=tmp
print(' '.join(map(str,n_)))