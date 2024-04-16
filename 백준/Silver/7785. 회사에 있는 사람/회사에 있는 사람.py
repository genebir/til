c={}
for _ in range(int(input())):
    x,y=input().split()
    c[x]=int(y=='enter')
for _ in sorted([k for k,v in c.items() if v==1], reverse=True):
    print(_)