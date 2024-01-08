def solution(N, stages):
    # l=[stages.count(_)/len([__ for __ in stages if __ >= _]) for _ in range(1,N+1)]
    l=[]
    for n in range(1,N+1):
        notclear=stages.count(n)
        users=len([_ for _ in stages if _>=n])
        failed = notclear/users if notclear>0 and users>0 else 0
        l.append(failed)

    d=dict(zip([_ for _ in range(1,N+1)],l))
    return sorted(d,key=d.get,reverse=True)