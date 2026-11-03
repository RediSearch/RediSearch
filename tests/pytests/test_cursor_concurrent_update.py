"""A write during pagination drops *other* documents from the scan.

No field TTLs, no debug commands, no cluster — a report paging with
`FT.AGGREGATE … WITHCURSOR` while the application serves an ordinary write.

Both the written document and its neighbours go missing, and neither is excused
by snapshot semantics: `test_document_moved_far_ahead_is_still_returned` writes
the same document to a distant value, re-indexing it exactly the same way, and it
comes back. A cursor does reach documents indexed after it started.
"""

from common import *

DOC_COUNT = 200
# Small pages: the loss is confined to the first block segment of the index, so
# a reader that clears it on page one never sees this.
PAGE_SIZE = 3
# First document of page two — the write has to land while the reader is still
# inside that first segment.
RESAVED = 4


def load_inventory(env):
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'inventory', 'ON', 'HASH', 'PREFIX', '1', 'item:',
               'SCHEMA', 'qty', 'NUMERIC').ok()
    for i in range(1, DOC_COUNT + 1):
        conn.execute_command('HSET', f'item:{i}', 'qty', str(i))
    return conn


def page_through(env, resave=None):
    rows, cursor = env.cmd('FT.AGGREGATE', 'inventory', '@qty:[0 100000]',
                           'LOAD', 1, '@__key', 'WITHCURSOR', 'COUNT', PAGE_SIZE)
    seen = [to_dict(row)['__key'] for row in rows[1:]]

    if resave is not None:
        # A full-object save: every field written back, value unchanged.
        getConnectionByEnv(env).execute_command('HSET', f'item:{resave}', 'qty', str(resave))

    while cursor:
        rows, cursor = env.cmd('FT.CURSOR', 'READ', 'inventory', cursor)
        seen += [to_dict(row)['__key'] for row in rows[1:]]
    return seen


def missing(seen):
    """Documents absent from the scan. All of them still match the range."""
    expected = {f'item:{i}' for i in range(1, DOC_COUNT + 1)}
    return sorted(expected - set(seen), key=lambda key: int(key.split(':')[1]))


@skip(cluster=True)
def test_paginated_scan_is_complete_without_concurrent_writes(env):
    """Control: nothing is written while the scan runs, so nothing is missing."""
    load_inventory(env)
    seen = page_through(env)
    env.assertEqual(len(seen), DOC_COUNT, message=f'returned {len(seen)}')


@skip(cluster=True)
def test_write_during_scan_does_not_drop_documents(env):
    """Writing one document must not remove documents from the scan.

    Only `item:4` is written, with the value it already had, so every document
    still matches the range throughout. Several never come back — including
    documents nobody touched.
    """
    load_inventory(env)
    seen = page_through(env, resave=RESAVED)

    lost = missing(seen)
    env.assertEqual(lost, [], message=f'{len(lost)} documents dropped from the scan: {lost}')


@skip(cluster=True)
def test_document_moved_far_ahead_is_still_returned(env):
    """The control that rules out snapshot semantics as an explanation.

    `item:4` is re-indexed here too — same write, same new version — but to a
    value far from where the reader is. It comes back, so a cursor plainly does
    reach documents indexed after it started, and the losses above are not the
    scan honouring a snapshot.
    """
    conn = load_inventory(env)
    rows, cursor = env.cmd('FT.AGGREGATE', 'inventory', '@qty:[0 100000]',
                           'LOAD', 1, '@__key', 'WITHCURSOR', 'COUNT', PAGE_SIZE)
    seen = [to_dict(row)['__key'] for row in rows[1:]]
    conn.execute_command('HSET', f'item:{RESAVED}', 'qty', '99999')
    while cursor:
        rows, cursor = env.cmd('FT.CURSOR', 'READ', 'inventory', cursor)
        seen += [to_dict(row)['__key'] for row in rows[1:]]

    env.assertContains(f'item:{RESAVED}', seen, message=f'returned {len(seen)}')
