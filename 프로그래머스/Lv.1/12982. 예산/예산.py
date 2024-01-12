def solution(d, budget):
    answer = 0
    while min(d)<=budget:
        budget-=d.pop(d.index(min(d)))
        answer+=1
        if len(d)==0:
            break
    return answer