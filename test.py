# from datetime import datetime

if __name__ == '__main__':


    '''
    d = datetime.now()
    print(str(d.year) + str(d.month) + str(d.day) + '_' + str(d.hour) + str(d.minute))
    
    v = list(range(1, 10))
    # print(v)
    y = list(range(5, 15))
    z = [{"1":"1"}, {"2":"2"}, {"3":"3"}]

    # = [x for x in membership if x not in defaults and not membership[x]]

    print( x for x in v if x not in y)
    '''

    ll = ['a', 'b', 'c', 'd']
    for ls in reversed(ll):
        print(ls)
        print("## : " + str(ll.index(ls)))
        ll.pop(ll.index(ls))
        print("$$ : " + ll.__str__())