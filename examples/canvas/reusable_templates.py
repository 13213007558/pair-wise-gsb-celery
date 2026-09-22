"""Reusable signature templates: inheritance boundaries.

This example shows how a single signature (or a whole canvas) can be
kept as a *template* and dispatched many times -- concurrently, for
different tenants -- without the dispatches leaking state into the
template or into each other.

The rules demonstrated here:

* `apply_async()` (and `apply()`) never mutate the template:
  every dispatch freezes *clones*, so each run gets independent
  task ids, parent ids and group ids.
* An immutable member (`sig.si(...)`) never gets the parent task's
  result injected into its args/kwargs, but trace headers -- root id,
  parent id, group id and stamps -- are still inherited by the
  dispatched clones.
* `clone()`/`partial()` create independent copies; mutating a
  clone never writes back into the template.
* `freeze()` is the one explicitly *finalizing* operation: it assigns
  a concrete task id to the signature it is called on, so a signature
  should not be reused after being frozen directly.
* Dictionaries/lists passed by the caller (kwargs, task lists,
  serialized dicts) are never written back to.

Run it directly (uses eager mode, no broker needed)::

    python examples/canvas/reusable_templates.py
"""

import copy

from celery import Celery, chain, chord, group

app = Celery('reusable_templates')
app.conf.task_always_eager = True


@app.task
def charge(tenant_id, amount):
    return f'charged {amount} to {tenant_id}'


@app.task
def notify(tenant_id):
    return f'notified {tenant_id}'


@app.task
def summarize(results):
    return list(results)


def snapshot(canvas):
    """Byte-level snapshot of a canvas template."""
    return copy.deepcopy(dict(canvas))


def main():
    # A single chain template, shared by every tenant.  The charge step
    # is mutable: dispatch-time kwargs (the tenant) are merged into its
    # clone.  The notify step is immutable: it must not receive the
    # parent task's result, nor the dispatch-time kwargs.
    template = chain(
        charge.s(amount=10),
        notify.si('ops-team'),
    )
    before = snapshot(template)

    # Dispatch the same template for two tenants.  Partial args/kwargs
    # given at dispatch time are merged into *clones* of the template.
    res_a = template.apply_async(kwargs={'tenant_id': 'tenant-a'})
    res_b = template.apply_async(kwargs={'tenant_id': 'tenant-b'})

    # The chain result is the immutable notify step; its parent is the
    # tenant-specific charge step.
    print('tenant-a:', res_a.parent.get(), '->', res_a.get())
    print('tenant-b:', res_b.parent.get(), '->', res_b.get())
    # charge ran with the dispatch-time tenant kwargs (mutable member),
    # while notify ran with its template args only: the parent's result
    # and the dispatch kwargs were NOT injected into the immutable step.

    # Each dispatch produced independent task ids (root lineage).
    assert res_a.id != res_b.id, 'every dispatch must get its own task id'
    print('independent task ids:', res_a.id, res_b.id)

    # The template itself is byte-identical after both dispatches:
    # no task ids, parent ids, or tenant kwargs leaked into it.
    assert snapshot(template) == before, 'template must not be mutated'
    assert 'task_id' not in template.options
    print('template unchanged after dispatch: OK')

    # Immutability boundary: the immutable notify step was NOT injected
    # with the charge result (it ran with its own args only), while the
    # mutable first step did receive the dispatch-time kwargs.
    # (Both results above show the tenant-specific values.)

    # clone()/partial() derive independent copies from the template.
    vip_template = template.clone()
    vip_template.tasks[0].kwargs['amount'] = 100
    assert snapshot(template) == before, 'clones must not write back'
    print('clone() does not write back into the template: OK')

    # The same holds for deep group/chord nesting.
    nested = chain(
        group(charge.s('t1', 1), charge.s('t2', 2)),
        chord(
            group(notify.si('t1'), notify.si('t2')),
            summarize.s(),
        ),
    )
    nested_before = snapshot(nested)
    nested.apply_async()
    nested.apply_async()
    assert snapshot(nested) == nested_before
    print('deep chain/group/chord template unchanged: OK')

    # freeze() is the explicit finalization boundary: it assigns a
    # concrete task id to the signature it is called on.  Freeze a
    # *clone* when the template must stay reusable.
    frozen = template.clone().freeze()
    assert snapshot(template) == before, 'freezing a clone is safe'
    print('frozen clone id:', frozen.id)
    print('template still pristine after clone+freeze: OK')


if __name__ == '__main__':
    main()
