while True:
    n = input().split(' ')
    a = int(n[0])
    b = int(n[1])
    if a + b == 0:
        break
    print(
        'factor' if b % a == 0 else 'multiple' if a % b == 0 else 'neither'
    )

