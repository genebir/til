cnt=0
N=int(input())
X=map(int, input().split(' '))
for i in X:
    cnt+=1 if len([j for j in range(1,i+1) if i%j==0])==2 else 0
print(cnt)