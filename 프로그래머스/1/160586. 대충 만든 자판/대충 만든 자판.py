def solution(keymap, targets):
    answer = []
    for s in targets:
        keycount=0
        for _ in s:
            keycounts=[key.find(_) for key in keymap if key.find(_)>=0]
            if len(keycounts)==0:
                keycount=-1
                break
            keycount+=min(keycounts)+1
        answer.append(keycount)
    return answer