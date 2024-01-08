def solution(k, m, score):
    answer=0
    tmp=[0]*(k+1)
    for _ in score:
        tmp[_]+=1
    tmp_box=[]
    for i,v in enumerate(tmp[::-1]):
        for _ in range(v):
            tmp_box.append(k-i)
            if len(tmp_box)==m:
                answer+=min(tmp_box)*m
                tmp_box=[]
    return answer