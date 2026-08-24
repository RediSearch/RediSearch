# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

import csv
from redisearch import Client


def index():
    client = Client('sh')
#    client.drop_index()
    client.create_index(txt=1.0)
    chapters = {}
    with open('will_play_text.csv') as fp:

        r = csv.reader(fp, delimiter=';')
        for line in r:
            #['62816', 'Merchant of Venice', '9', '3.2.74', 'PORTIA', "I'll begin it,--Ding, dong, bell."]

            play, chapter, character, text = line[1], line[2], line[4], line[5]

            d = chapters.setdefault('{}:{}'.format(play, chapter), {})
            d['play'] = play
            d['text'] = d.get('text', '') + ' ' + text

    for chapter, doc in chapters.iteritems():
        print chapter, doc
        client.add_document(chapter, nosave=True, txt=doc['text'])

if __name__ == '__main__':
    index()
