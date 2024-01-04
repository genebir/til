c=0
for _ in range(int(input())):
    tl=[]
    tw=''
    w=list(input())
    flag=False
    for __ in w:
        if __ not in tl:
            tl.append(__)
            tw=__
        elif __==tw:
            continue
        else:
            flag=True
            break
    if not flag:
        c += 1
print(c)