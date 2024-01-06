def solution(t, p):
    return len([_ for _ in [t[i:i+len(p)] for i in range(len(t)-len(p)+1)] if p>=_])