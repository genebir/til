N,M=map(int,input().split(' '))
l1=[list(map(int,input().split(' '))) for _ in range(N)]
l2=[list(map(int,input().split(' '))) for _ in range(N)]
for i in range(N):
    tmp = []
    for j in range(M):
        tmp.append(str(l1[i][j]+l2[i][j]))
    print(' '.join(tmp))