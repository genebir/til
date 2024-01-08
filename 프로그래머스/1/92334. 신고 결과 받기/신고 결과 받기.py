def solution(id_list, report, k):
    answer = []
    reported=dict(zip(id_list,[[] for _ in range(len(id_list))]))
    mailing=dict(zip(id_list,[0 for _ in range(len(id_list))]))
    report={*report}
    for _ in report:
        u,r=_.split()
        reported[r].append(u)
    for v in reported.values():
        if len(v)<k:
            continue
        for _ in v:
            mailing[_] = mailing.get(_,0)+1
    return [*mailing.values()]