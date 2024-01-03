def fibo(x,y,cnt,end):
    if cnt==end:
        return x
    else:
        return fibo(y,x+y,cnt+1,end)
print(fibo(0,1,0,int(input())))