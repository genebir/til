def solution(wallpaper):
    min_x=50
    min_y=50
    max_x=0
    max_y=0
    for i,v in enumerate(wallpaper):
        min_x=i if '#' in v and i<min_x else min_x
        min_y=v.find('#') if v.find('#')<min_y and v.find('#')>=0 else min_y
        max_x=i if '#' in v and i>max_x else max_x
        max_y=v.rfind('#') if v.rfind('#')>max_y else max_y
    max_x+=1
    max_y+=1
    return [min_x,min_y,max_x,max_y]