x=list(input().lower())
tmp={}
for _ in x:
    tmp[_] = tmp.get(_, 0) + 1
_max = [k for k,v in tmp.items() if max(tmp.values())==v]
print(_max[0].upper() if len(_max)==1 else '?')