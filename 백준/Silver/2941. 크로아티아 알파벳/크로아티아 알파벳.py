ca=['c=','c-','dz=','d-','lj','nj','s=','z=']
s=input()
for c in ca:
    s=s.replace(c,'_')
print(len(s))