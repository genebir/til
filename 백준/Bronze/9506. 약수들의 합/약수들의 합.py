while True:
    a=int(input())
    if a == -1:
        break
    l=[i for i in range(1, a) if a % i == 0]
    print(f"{a} = {' + '.join(map(str,l))}" if sum(l)==a else f"{a} is NOT perfect.")