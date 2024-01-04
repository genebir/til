for _ in range(int(input())):
    g=int(input());a,g=divmod(g,25);b,g=divmod(g,10);c,g=divmod(g,5);print(a,b,c,g)