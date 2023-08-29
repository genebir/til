a, b = map(int,input().split(' '))
l = [i for i in range(1, a+1) if a % i == 0]
print(l[b-1] if len(l) > b-1 else 0)