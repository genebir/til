d={}
for _ in input().split():
    d[_]=d.get(_,0)+1
m=max(d.values())
for k,v in d.items():
    if m==3:
        print(int(k)*1000+10000)
        break
    if m==2 and v==2:
        print(int(k)*100+1000)
        break
    if m==1:
        print(int(max(d.keys()))*100)
        break