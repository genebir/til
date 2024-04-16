d={}
n,m=map(int,input().split())
nm=[]
for _ in range(n+m):
    tmp=input()
    d[tmp]=d.get(tmp,0)+1
for k,v in d.items():
    if v>1:
        nm.append(k)
print(len(nm))
for _ in sorted(nm):
    print(_)