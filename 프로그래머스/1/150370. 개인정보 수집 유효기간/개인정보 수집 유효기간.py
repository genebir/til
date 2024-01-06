def solution(today, terms, privacies):
    from datetime import datetime
    from dateutil.relativedelta import relativedelta
    st=lambda x: datetime.strptime(x,'%Y.%m.%d')
    today=st(today)
    d=dict(map(lambda x: (x.split()[0], relativedelta(months=int(x.split()[1]))),terms))
    answer = []
    for i,v in enumerate(privacies):
        a,b=v.split()
        answer.append(i+1) if today>=st(a)+d[b] else None
    return answer