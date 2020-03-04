pd = {'/a': 1, '/a/b': 2, '/a/c': 2, '/a/c/d': 3, '/a/c/e': 3, '/a/c/d/f': 4}
ds1 = ['a', 'b', 'c', 'd', 'f', 'e']
ds2 = ['a', 'b', 'c', 'X', 'e', 'd', 'f']
ds3 = ['a', 'c', 'd', 'f', 'b', 'c', 'e']
ds4 = ['a', 'c', 'd', 'f', 'e', 'b']
ds5 = ['a', 'c', 'e', 'b', 'c', 'd', 'f']
ds6 = ['a', 'c', 'e', 'd', 'f', 'b']



cp = []
ll = 0

for h in ds2:

    print(f'current heading << {h} >>')
    sps = []
    for i in range(len(cp), -1, -1):
        tp = cp[:i] + [h]
        sps.append(tp)
    spins = ['/' + '/'.join(hs) for hs in sps]

    # -------------------------
    # print(f'search paths to try: {spins}')
    f = False
    for sps in spins:
        if sps in pd:
            f = True
            # print(f'found path: {sps}')
            break
    # -------------------------

    if f:
        if pd[sps] > ll:
            cp.append(h)
        else:
            cp = cp[:(pd[sps] - ll - 1)] + [h]
        print(f'****   current path:     {cp}')
        ll = pd[sps]
    else:
        print(f'cannot find existing path for heading {h}')
