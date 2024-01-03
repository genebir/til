spg={'A+':	4.5,
'A0':	4.0,
'B+':	3.5,
'B0':	3.0,
'C+':	2.5,
'C0':	2.0,
'D+':	1.5,
'D0':	1.0,
'F':	0.0}
a=[input().split(' ') for _ in range(20)]
ts=0
ms=0
for _ in a:
    a,b,c=_
    ts+=float(b) if c!='P' else 0
    ms+=(float(b)*spg[c]) if c!='P' else 0
print(round(ms/ts, 6))