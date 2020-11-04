from redis import Redis, RedisError, ConnectionPool
import datetime
import itertools
import json
import time


class Document(object):

    def __init__(self, id, **fields):

        self.id = id
        for k, v in fields.iteritems():
            setattr(self, k, v)

    def __repr__(self):

        return 'Document %s' % self.__dict__


    def snippetize(self, field, size=500, boldTokens=[]):
        txt = getattr(self, field, '')
        for tok in boldTokens:
            txt = txt.replace(tok, "<b>%s</b>" % tok)
        while size < len(txt) and txt[size] != ' ':
            size+=1

        setattr(self, field, (txt[:size] + '...') if len(txt) > size else txt)

class Result(object):

    def __init__(self, res, hascontent, queryText, duration=0):

        self.total = res[0]
        self.duration = duration
        self.docs = []

        tokens = filter(None, queryText.rstrip("\" ").lstrip(" \"").split(' '))
        for i in xrange(1, len(res), 2 if hascontent else 1):
            id = res[i]
            fields = {} 
            if hascontent:
                fields = dict(
                    dict(itertools.izip(res[i + 1][::2], res[i + 1][1::2]))) if hascontent else {}
            try:
                del fields['id']
            except KeyError:
                pass

            doc = Document(id, **fields)
            #print doc
            if hascontent:
                try:
                    doc.snippetize('body', size=500, boldTokens = tokens)
                except Exception as e:
                    print e
            self.docs.append(doc)


    def __repr__(self):

        return 'Result{%d total, docs: %s}' % (self.total, self.docs)


class Client(object):

    NUMERIC = 'numeric'

    CREATE_CMD = 'FT.CREATE'
    SEARCH_CMD = 'FT.SEARCH'
    ADD_CMD = 'FT.ADD'
    DROP_CMD = 'FT.DROP'


    class BatchIndexer(object):
        """
        A batch indexer allows you to automatically batch 
        document indexeing in pipelines, flushing it every N documents. 
        """

        def __init__(self, client, chunk_size = 1000):

            self.client = client
            self.pipeline = client.redis.pipeline(False)
            self.total = 0
            self.chunk_size = chunk_size
            self.current_chunk = 0

        def __del__(self):
            if self.current_chunk:
                self.commit()
        
        def add_document(self, doc_id, nosave = False, score=1.0, **fields):

            self.client._add_document(doc_id, conn=self.pipeline, nosave = nosave, score = score, **fields)
            self.current_chunk += 1
            self.total += 1
            if self.current_chunk >= self.chunk_size:
                self.commit()
                

        def commit(self):
            
            self.pipeline.execute()
            self.current_chunk = 0

    def __init__(self, index_name, host='localhost', port=6379):
        self.host = host
        self.port = port
        self.index_name = index_name

        self.redis = Redis(
            connection_pool = ConnectionPool(host=host, port=port))

    def batch_indexer(self, chunk_size = 100):
        """
        Create a new batch indexer from the client with a given chunk size
        """
        return Client.BatchIndexer(self, chunk_size = chunk_size)
    
    def create_index(self, **fields):
        """
        Create the search index. Creating an existing index juts updates its properties
        :param fields: a kwargs consisting of field=[score|NUMERIC]
        :return:
        """
        self.redis.execute_command(
            self.CREATE_CMD, self.index_name, *itertools.chain(*fields.items()))

    def drop_index(self):
        """
        Drop the index if it exists
        :return:
        """
        self.redis.execute_command(self.DROP_CMD, self.index_name)

    def _add_document(self, doc_id, conn = None, nosave = False, score=1.0, **fields):
        """ 
        Internal add_document used for both batch and single doc indexing 
        """
        if conn is None:
            conn = self.redis

        args = [self.ADD_CMD, self.index_name, doc_id, score]
        if nosave:
            args.append('NOSAVE')
        args.append('FIELDS') 
        args += list(itertools.chain(*fields.items()))
        return conn.execute_command(*args)

    def add_document(self, doc_id, nosave = False, score=1.0, **fields):
        """
        Add a single document to the index.
        :param doc_id: the id of the saved document.
        :param nosave: if set to true, we just index the document, and don't save a copy of it. 
                       this means that searches will just return ids.
        :param score: the document ranking, between 0.0 and 1.0. 
        :fields: kwargs dictionary of the document fields to be saved and/or indexed 
        """
        return self._add_document(doc_id, conn=None, nosave=nosave, score=score, **fields)

    def load_document(self, id):
        """
        Load a single document by id
        """
        fields = self.redis.hgetall(id)
        try:
            del fields['id']
        except KeyError:
            pass

        return Document(id=id, **fields)


    def search(self, query, offset =0, num = 10, verbatim = False, no_content=False, no_stopwords = False, fields=None, **filters):
        """
        Search eht
        :param query:
        :param fields:
        :param filters:
        :return:
        """

        args = [self.index_name, query]
        if no_content:
            args.append('NOCONTENT')

        if fields:

            args.append('INFIELDS')
            args.append(len(fields))
            args += fields
        
        if verbatim:
            args.append('VERBATIM')

        if no_stopwords:
            args.append('NOSTOPWORDS')

        if filters:
            for k, v in filters.iteritems():
                args += ['FILTER', k] + list(v)

        args += ["LIMIT", offset, num]

        st = time.time()
        res = self.redis.execute_command(self.SEARCH_CMD, *args)

        return Result(res,  no_content == False, queryText=query, duration = (time.time()-st)*1000.0)
