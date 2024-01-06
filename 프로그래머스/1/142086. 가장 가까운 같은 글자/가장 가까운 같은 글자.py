def solution(s):
    answer = []
    for i in range(len(s)):
        x=s[:i].rfind(s[i])
        answer.append(i-x if x!=-1 else x)
    return answer