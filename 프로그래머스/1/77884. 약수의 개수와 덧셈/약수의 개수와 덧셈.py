def solution(left, right):
    answer = 0
    for i in range(left,right+1):
        a=len([j for j in range(1,i+1) if i%j==0])%2
        answer+=i if a==0 else -i
    return answer