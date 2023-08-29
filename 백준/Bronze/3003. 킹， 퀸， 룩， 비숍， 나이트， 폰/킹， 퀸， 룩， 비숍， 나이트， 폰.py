chess=[1,1,2,2,2,8]
X = list(map(int, input().split(' ')))
print(' '.join(list(str(x-y) for x,y in zip(chess,X))))