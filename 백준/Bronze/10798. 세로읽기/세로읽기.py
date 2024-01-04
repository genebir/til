l=[input() for _ in range(5)]
max_l=max([len(_) for _ in l])
s=''
for i in range(max_l):
    for v in l:
        s=s+v[i] if len(v)>i else s
print(s)