import sys
input = sys.stdin.readline

N,M=map(int,input().split())
result=[]
visited=[False]*(N+1)

def recur(n):
    if n == M:
        print(' '.join(map(str, result)))
        return
    for i in range(1, N+1):
        if not visited[i]:
            visited[i] = True
            result.append(i)
            recur(n+1)
            visited[i] = False
            result.pop()

recur(0)
