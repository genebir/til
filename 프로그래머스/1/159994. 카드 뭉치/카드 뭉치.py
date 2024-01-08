def solution(cards1, cards2, goal):
    answer = ''
    for _ in goal:
        a,b=cards1[0] if len(cards1)>0 else '',cards2[0] if len(cards2)>0 else ''
        if _==a:
            cards1.pop(0)
        elif _==b:
            cards2.pop(0)
        elif _ not in (a,b):
            answer='No'
            break
        answer='Yes'
    return answer