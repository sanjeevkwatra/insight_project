
def seriesA_alert(x):
    maxval= None
    minval=None 
    for state in x:
        if state is None:
            continue
        if maxval is None:
           maxval= state[0]
           minval= state[1]
        else: 
           maxval= max(state[0], maxval)
           minval= min(state[1], minval)
    if maxval is None:
        return ("Alert","No data")
    if maxval -minval > 7:
        return ("Max", maxval, "Min", minval, "Alert", "Range more than 5") 
    return ("Max", maxval, "Min", minval)


def seriesB_alert(x):
    total= None
    for state in x:
        if state is None:
            continue
        if total is None:
           total= state[2]
           count= state[3]
        else :
           total+= state[2]
           count+= state[3]
    if total is None:
        return ("Alert","No data")
    avg=  float(total)/count
    if avg < 3:
        return ("Sum", total, "Count", count, "Avg", avg, "Alert", "Averag is < 3")
    return ("Sum", total, "Count", count, "Avg", avg)

alert_funcs= {'seriesA_alert':seriesA_alert, 'seriesB_alert':seriesB_alert}

