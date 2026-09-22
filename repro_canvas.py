"""Repro: canvas template reuse must not mutate the shared template."""
import copy
import pickle
import threading

from celery import Celery, chain, chord, group, signature
from celery.signals import before_task_publish

app = Celery('repro', broker='memory://', backend='cache+memory://')
app.conf.task_default_queue = 'repro'


@app.task(bind=True)
def add(self, x, y):
    return x + y


published = []


def on_pub(sender=None, body=None, headers=None, properties=None, **kw):
    published.append({'body': copy.deepcopy(body),
                      'headers': copy.deepcopy(headers)})


before_task_publish.connect(on_pub)


def snap(sig):
    return pickle.dumps(sig)


def build_template():
    return chain(
        add.s(1, 2),
        group(add.s(3), add.si(4, 5)),
        add.si(6, 7),
    )


def collect_ids():
    ids = []
    for msg in published:
        body = msg['body']
        if isinstance(body, tuple) and len(body) == 3:
            ids.append(msg['headers']['id'])
    return ids


failures = []


def check(name, cond, extra=''):
    print(('PASS' if cond else 'FAIL'), name, extra)
    if not cond:
        failures.append(name)


# 1. apply_async twice: template must stay byte-identical, ids must differ
template = build_template()
s0 = snap(template)
published.clear()
template.apply_async()
ids1 = collect_ids()
published.clear()
template.apply_async()
ids2 = collect_ids()
check('template unchanged after 2x apply_async', snap(template) == s0)
check('independent task ids across dispatches',
      not set(ids1) & set(ids2) and ids1 and ids2,
      f'{ids1} vs {ids2}')

# 2. clone().freeze() must not pollute the template
published.clear()
c = template.clone()
c.freeze()
check('template unchanged after clone().freeze()', snap(template) == s0)

# 3. freeze() of the template itself must not leak task ids into later
#    dispatches (template reuse after freeze)
published.clear()
template.freeze()
leaked = snap(template) != s0
print('INFO template mutated by own freeze():', leaked)
published.clear()
template.apply_async()
ids3 = collect_ids()
check('ids after template.freeze() still independent',
      not set(ids3) & (set(ids1) | set(ids2)) and ids3,
      f'{ids3}')

# 4. concurrent dispatch from threads
template2 = build_template()
s1 = snap(template2)
published.clear()
errors = []


def worker():
    try:
        template2.apply_async()
    except Exception as exc:  # noqa
        errors.append(exc)


threads = [threading.Thread(target=worker) for _ in range(8)]
for t in threads:
    t.start()
for t in threads:
    t.join()
ids_c = collect_ids()
check('no errors in concurrent dispatch', not errors, str(errors[:1]))
check('template unchanged after concurrent dispatch', snap(template2) == s1)
check('all concurrent ids unique', len(set(ids_c)) == len(ids_c) and ids_c,
      f'{len(ids_c)} ids, {len(set(ids_c))} unique')

# 5. chord template reuse
ct = chord([add.s(1, 1), add.si(2, 2)], add.si(0))
sc = snap(ct)
published.clear()
ct.apply_async()
ct.apply_async()
check('chord template unchanged after 2x apply_async', snap(ct) == sc)

# 6. group template reuse
gt = group(add.s(1, 1), add.si(2, 2))
sg = snap(gt)
gt.apply_async()
gt.apply_async()
check('group template unchanged after 2x apply_async', snap(gt) == sg)

# 7. group.freeze() must not pollute
gt2 = group(add.s(1, 1), add.si(2, 2))
sg2 = snap(gt2)
gt2.freeze()
print('INFO group mutated by own freeze():', snap(gt2) != sg2)

# 8. user dict must not be written back by signature()/from_dict
d = {
    'task': 'repro.add', 'args': (1, 2), 'kwargs': {},
    'options': {}, 'subtask_type': None, 'immutable': False,
}
d0 = copy.deepcopy(d)
signature(d)
check('user dict not mutated by signature()', d == d0)

gd = {
    'task': 'celery.group', 'args': (),
    'kwargs': {'tasks': [dict(d0), dict(d0)]},
    'options': {}, 'subtask_type': 'group', 'immutable': False,
}
gd0 = copy.deepcopy(gd)
signature(gd)
check('user group dict not mutated by signature()', gd == gd0)

cd = {
    'task': 'celery.chain', 'args': (),
    'kwargs': {'tasks': (dict(d0), dict(d0))},
    'options': {}, 'subtask_type': 'chain', 'immutable': False,
}
cd0 = copy.deepcopy(cd)
signature(cd)
check('user chain dict not mutated by signature()', cd == cd0)

# 9. immutable member of chain must not receive parent result (eager)
eager = Celery('eager')
eager.conf.task_always_eager = True
eager.conf.task_eager_propagates = True
seen = []


@eager.task(bind=True)
def rec(self, *args, **kwargs):
    seen.append((self.request.id, self.request.parent_id,
                 self.request.root_id, args, kwargs))
    return args[0] if args else None


@eager.task(bind=True)
def produce(self):
    return 42


seen.clear()
chain(produce.s(), rec.si(7, 8), rec.s(9)).apply_async()
check('immutable task not injected with parent result',
      seen[0][3] == (7, 8), f'got {seen[0][3]}')
check('mutable task got parent result', seen[1][3] == (7, 9),
      f'got {seen[1][3]}')
check('root lineage shared in eager chain',
      seen[0][2] == seen[1][2] and seen[0][2],
      f'{[s[2] for s in seen]}')
check('parent linkage in eager chain',
      seen[1][1] == seen[0][0] and seen[0][1] is not None,
      f'{[(s[0], s[1]) for s in seen]}')

print()
print('FAILURES:', failures or 'none')
