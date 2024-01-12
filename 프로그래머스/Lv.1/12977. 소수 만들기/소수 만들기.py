def solution(nums):
    answer = 0
    for i,v in enumerate(nums):
        for j in range(i+1,len(nums)):
            for k in range(j+1,len(nums)):
                tot=sum([v,nums[j],nums[k]])
                answer+=1 if len([_ for _ in range(1,int(tot**0.5)+1) if tot%_==0])==1 else 0
    return answer