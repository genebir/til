import sys
input=sys.stdin.readline

N   = int(input())                                     # 줄 수
map = [list(map(int,input().strip())) for _ in range(N)] # 지도
chk = [[False]*N for _ in range(N)]                    # 방문여부
cnt = 0                                                # 총 단지 수
rst = []                                               # 결과
dy, dx = [0,1,0,-1],[1,0,-1,0]
e = 0

def dfs(y,x):
    global e
    e += 1
    for k in range(4):
        ny = y + dy[k]
        nx = x + dx[k]
        
        if 0<=ny<N and 0<=nx<N:
            if map[ny][nx] > 0 and not chk[ny][nx]:
                chk[ny][nx] = True
                dfs(ny, nx)
                

for j in range(N):
    for i in range(N):
        if map[j][i] == 1 and not chk[j][i]:
            chk[j][i] = True
            cnt += 1
            e = 0
            dfs(j,i)
            rst.append(e)
            
rst.sort()
print(cnt)
for _ in rst:
    print(_)