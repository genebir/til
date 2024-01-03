l=[input() for _ in range(int(input()))]
for i in l:
    n=0
    _i=list(i)
    for _ in range(len(_i)):
        n = (n+1) if _i.pop()==')' else (n-1)
        if n==-1:
            break
    print('YES' if n==0 else 'NO')