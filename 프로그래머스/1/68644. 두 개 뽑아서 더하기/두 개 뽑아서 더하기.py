def solution(numbers):
    answer = []
    for i,v in enumerate(numbers):
        for _ in range(i+1,len(numbers)):
            answer.append(v+numbers[_])
    return sorted([*set(answer)])