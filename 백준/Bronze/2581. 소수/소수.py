M=int(input())
N=int(input())
l=[]
for i in range(M,N+1):
    l.append(i) if len([j for j in range(1,i+1) if i%j==0])==2 else 0
print(-1) if len(l)==0 else print(f"{sum(l)}\n{min(l)}")