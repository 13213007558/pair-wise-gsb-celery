import copy

from celery import Celery, chain, chord, group

app = Celery('repro', broker='memory://', backend='cache+memory://')


@app.task(bind=True)
def add(self, x, y):
    return x + y


def diff(a, b, path=''):
    if type(a) is not type(b):
        print('TYPE', path, type(a), type(b))
        return
    if isinstance(a, dict):
        for k in set(a) | set(b):
            if k not in a:
                print('ADDED', path + '/' + str(k), '=', b[k])
            elif k not in b:
                print('REMOVED', path + '/' + str(k))
            else:
                diff(a[k], b[k], path + '/' + str(k))
    elif isinstance(a, (list, tuple)):
        if len(a) != len(b):
            print('LEN', path, len(a), len(b))
            return
        for i, (x, y) in enumerate(zip(a, b)):
            diff(x, y, path + f'[{i}]')
    else:
        if a != b:
            print('VAL', path, repr(a), '->', repr(b))


template = chain(add.s(1, 2), group(add.s(3), add.si(4, 5)), add.si(6, 7))
before = copy.deepcopy(dict(template))
template.apply_async()
print('--- chain apply_async diff ---')
diff(before, dict(template))

template2 = chain(add.s(1, 2), group(add.s(3), add.si(4, 5)), add.si(6, 7))
before2 = copy.deepcopy(dict(template2))
c = template2.clone()
c.freeze()
print('--- clone().freeze() diff ---')
diff(before2, dict(template2))

ct = chord([add.s(1, 1), add.si(2, 2)], add.si(0))
before3 = copy.deepcopy(dict(ct))
ct.apply_async()
print('--- chord apply_async diff ---')
diff(before3, dict(ct))
