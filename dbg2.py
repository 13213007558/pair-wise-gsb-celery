import copy
import pickle

from celery import Celery, chord

app = Celery('repro', broker='memory://', backend='cache+memory://')


@app.task(bind=True)
def add(self, x, y):
    return x + y


ct = chord([add.s(1, 1), add.si(2, 2)], add.si(0))
p0 = pickle.dumps(ct)
print('header list id:', id(ct.kwargs['header']))
ct.apply_async()
p1 = pickle.dumps(ct)
print('pickle equal after 1st apply_async:', p0 == p1)
print('header list id after:', id(ct.kwargs['header']))
h0 = ct.kwargs['header'][0]
print('header[0] type:', type(h0).__name__)
print('header[0] options:', h0.options)
print('body options:', ct.kwargs['body'].options)
print('chord options:', ct.options)
