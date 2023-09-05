N=int(input())
for _ in range(N):
    i,s,*_=input().split(' ')
    print(''.join([c*int(i) for c in s]))