def factorial(x,tot):
    if x<=1:
        return tot
    else:
        return factorial(x-1,x*tot)
print(factorial(int(input()),1))