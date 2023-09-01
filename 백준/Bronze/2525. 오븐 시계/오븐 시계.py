h,m=map(int,input().split(' '))
x=int(input())
h+=(m+x)//60
m+=x-(60*((m+x)//60))
print(f"{h-24 if h>23 else h} {m}")