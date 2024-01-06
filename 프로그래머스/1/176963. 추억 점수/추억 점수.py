def solution(name, yearning, photo):
    d=dict(zip(name,yearning))
    l=[]
    for p in photo:
        l.append(sum([d.get(_,0) for _ in p]))
    return l