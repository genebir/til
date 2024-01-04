def solution(a, b):
    days=['SUN','MON','TUE','WED','THU','FRI','SAT']
    day_=5
    monthes={
        '1':31,
        '2':29,
        '3':31,
        '4':30,
        '5':31,
        '6':30,
        '7':31,
        '8':31,
        '9':30,
        '10':31,
        '11':30,
        '12':31
    }
    day=0
    for k,v in monthes.items():
        if k==str(a):
            break
        else:
            day+=v
    day+=b-1
    for _ in range(day):
        day_=day_+1 if day_<6 else 0
        print(day_)
    return days[day_]