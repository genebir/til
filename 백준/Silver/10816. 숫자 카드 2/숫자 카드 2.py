d={};input()
for _ in input().split():
    d[_]=d.get(_,0)+1
input()
print(*[d.get(_,0) for _ in input().split()])