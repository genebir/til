import sys

input = sys.stdin.readline

n, m = map(int, input().split())
mymap = [list(map(int, input().split())) for _ in range(n)]
chk = [[False] * m for _ in range(n)]

dy, dx = [0, 1, 0, -1], [1, 0, -1, 0]

cnt = 0
max_value = 0


def bfs(y, x):
    rs = 1
    q = [(y, x)]
    while q:
        ey, ex = q.pop()
        for k in range(4):
            ny = ey + dy[k]
            nx = ex + dx[k]
            if 0 <= ny < n and 0 <= nx < m:
                if mymap[ny][nx] == 1 and not chk[ny][nx]:
                    rs += 1
                    chk[ny][nx] = True
                    q.append((ny, nx))
    return rs


for j in range(n):
    for i in range(m):
        if mymap[j][i] == 1 and not chk[j][i]:
            cnt += 1
            chk[j][i] = True
            max_value = max(max_value, bfs(j, i))

print(cnt)
print(max_value)