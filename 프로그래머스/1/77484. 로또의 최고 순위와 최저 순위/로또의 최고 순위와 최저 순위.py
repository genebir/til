def solution(lottos, win_nums):
    s,s_=len([l for l in lottos if l not in win_nums]),len([l for l in lottos if l not in win_nums and l>0])
    return sorted([*map(lambda x: x+1 if x<6 else x, [s,s_])])
