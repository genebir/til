def recursion(s, l, r, c):
    if l >= r: print(1, c)
    elif s[l] != s[r]: print(0, c)
    else: return recursion(s, l+1, r-1, c+1)
def isPalindrome(s):
    return recursion(s, 0, len(s)-1, 1)
[isPalindrome(input()) for _ in range(int(input()))]