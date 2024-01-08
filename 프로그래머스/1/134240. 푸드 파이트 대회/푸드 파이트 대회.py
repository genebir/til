def solution(food):
    answer = ''
    for i,f in enumerate(food):
        f=f//2
        if f < 1:
            continue
        answer+=(str(i)*f)
    answer=answer+"0"+answer[::-1]
    return answer