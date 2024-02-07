from common import skipOnCrdtEnv

def createRdb(env, q):
  r = env.getConnection().pipeline()

  a_list = ['A', 'DSL', 'for', 'Abstract', 'Data', 'Types.', 'Redis', 'is', 'a', 'DSL', '(Domain', 'Specific', 'Language)', 'that', 'manipulates', 'abstract', 'data', 'types', 'and', 'implemented', 'as', 'a', 'TCP', 'daemon.', 'Commands', 'manipulate', 'a', 'key', 'space', 'where', 'keys', 'are', 'binary-safe', 'strings', 'and', 'values', 'are', 'different', 'kinds', 'of', 'abstract', 'data', 'types.', 'Every', 'data', 'type', 'represents', 'an', 'abstract', 'version', 'of', 'a', 'fundamental', 'data', 'structure.', 'For', 'instance', 'Redis', 'Lists', 'are', 'an', 'abstract', 'representation', 'of', 'linked', 'lists.', 'In', 'Redis,', 'the', 'essence', 'of', 'a', 'data', 'type', 'isnt', 'just', 'the', 'kind', 'of', 'operations', 'that', 'the', 'data', 'types', 'support,', 'but', 'also', 'the', 'space', 'and', 'time', 'complexity', 'of', 'the', 'data', 'type', 'and', 'the', 'operations', 'performed', 'upon', 'it.']
  b_list = ['Memory', 'storage', 'is', '#1.', 'The', 'Redis', 'data', 'set,', 'composed', 'of', 'defined', 'key-value', 'pairs,', 'is', 'primarily', 'stored', 'in', 'the', 'computers', 'memory.', 'The', 'amount', 'of', 'memory', 'in', 'all', 'kinds', 'of', 'computers,', 'including', 'entry-level', 'servers,', 'is', 'increasing', 'significantly', 'each', 'year.', 'Memory', 'is', 'fast,', 'and', 'allows', 'Redis', 'to', 'have', 'very', 'predictable', 'performance.', 'Datasets', 'composed', 'of', '10k', 'or', '40', 'millions', 'keys', 'will', 'perform', 'similarly.', 'Complex', 'data', 'types', 'like', 'Redis', 'Sorted', 'Sets', 'are', 'easy', 'to', 'implement', 'and', 'manipulate', 'in', 'memory', 'with', 'good', 'performance,', 'making', 'Redis', 'very', 'simple.', 'Redis', 'will', 'continue', 'to', 'explore', 'alternative', 'options', '(where', 'data', 'can', 'be', 'optionally', 'stored', 'on', 'disk,', 'say)', 'but', 'the', 'main', 'goal', 'of', 'the', 'project', 'remains', 'the', 'development', 'of', 'an', 'in-memory', 'database.']
  c_list = ['Fundamental', 'data', 'structures', 'for', 'a', 'fundamental', 'API.', 'The', 'Redis', 'API', 'is', 'a', 'direct', 'consequence', 'of', 'fundamental', 'data', 'structures.', 'APIs', 'can', 'often', 'be', 'arbitrary', 'but', 'not', 'an', 'API', 'that', 'resembles', 'the', 'nature', 'of', 'fundamental', 'data', 'structures.', 'If', 'we', 'ever', 'meet', 'intelligent', 'life', 'forms', 'from', 'another', 'part', 'of', 'the', 'universe,', 'theyll', 'likely', 'know,', 'understand', 'and', 'recognize', 'the', 'same', 'basic', 'data', 'structures', 'we', 'have', 'in', 'our', 'computer', 'science', 'books.', 'Redis', 'will', 'avoid', 'intermediate', 'layers', 'in', 'API,', 'so', 'that', 'the', 'complexity', 'is', 'obvious', 'and', 'more', 'complex', 'operations', 'can', 'be', 'performed', 'as', 'the', 'sum', 'of', 'the', 'basic', 'operations.']
  d_list = ['We', 'believe', 'in', 'code', 'efficiency.', 'Computers', 'get', 'faster', 'and', 'faster,', 'yet', 'we', 'believe', 'that', 'abusing', 'computing', 'capabilities', 'is', 'not', 'wise:', 'the', 'amount', 'of', 'operations', 'you', 'can', 'do', 'for', 'a', 'given', 'amount', 'of', 'energy', 'remains', 'anyway', 'a', 'significant', 'parameter:', 'it', 'allows', 'to', 'do', 'more', 'with', 'less', 'computers', 'and,', 'at', 'the', 'same', 'time,', 'having', 'a', 'smaller', 'environmental', 'impact.', 'Similarly', 'Redis', 'is', 'able', 'to', '"scale', 'down"', 'to', 'smaller', 'devices.', 'It', 'is', 'perfectly', 'usable', 'in', 'a', 'Raspberry', 'Pi', 'and', 'other', 'small', 'ARM', 'based', 'computers.', 'Faster', 'code', 'having', 'just', 'the', 'layers', 'of', 'abstractions', 'that', 'are', 'really', 'needed', 'will', 'also', 'result,', 'often,', 'in', 'more', 'predictable', 'performances.', 'We', 'think', 'likewise', 'about', 'memory', 'usage,', 'one', 'of', 'the', 'fundamental', 'goals', 'of', 'the', 'Redis', 'project', 'is', 'to', 'incrementally', 'build', 'more', 'and', 'more', 'memory', 'efficient', 'data', 'structures,', 'so', 'that', 'problems', 'that', 'were', 'not', 'approachable', 'in', 'RAM', 'in', 'the', 'past', 'will', 'be', 'perfectly', 'fine', 'to', 'handle', 'in', 'the', 'future.']
  e_list = ['Code', 'is', 'like', 'a', 'poem;', 'its', 'not', 'just', 'something', 'we', 'write', 'to', 'reach', 'some', 'practical', 'result.', 'Sometimes', 'people', 'that', 'are', 'far', 'from', 'the', 'Redis', 'philosophy', 'suggest', 'using', 'other', 'code', 'written', 'by', 'other', 'authors', '(frequently', 'in', 'other', 'languages)', 'in', 'order', 'to', 'implement', 'something', 'Redis', 'currently', 'lacks.', 'But', 'to', 'us', 'this', 'is', 'like', 'if', 'Shakespeare', 'decided', 'to', 'end', 'Enrico', 'IV', 'using', 'the', 'Paradiso', 'from', 'the', 'Divina', 'Commedia.', 'Is', 'using', 'any', 'external', 'code', 'a', 'bad', 'idea?', 'Not', 'at', 'all.', 'Like', 'in', '"One', 'Thousand', 'and', 'One', 'Nights"', 'smaller', 'self', 'contained', 'stories', 'are', 'embedded', 'in', 'a', 'bigger', 'story,', 'well', 'be', 'happy', 'to', 'use', 'beautiful', 'self', 'contained', 'libraries', 'when', 'needed.', 'At', 'the', 'same', 'time,', 'when', 'writing', 'the', 'Redis', 'story', 'were', 'trying', 'to', 'write', 'smaller', 'stories', 'that', 'will', 'fit', 'in', 'to', 'other', 'code.']
  f_list = ['Were', 'against', 'complexity.', 'We', 'believe', 'designing', 'systems', 'is', 'a', 'fight', 'against', 'complexity.', 'Well', 'accept', 'to', 'fight', 'the', 'complexity', 'when', 'its', 'worthwhile', 'but', 'well', 'try', 'hard', 'to', 'recognize', 'when', 'a', 'small', 'feature', 'is', 'not', 'worth', '1000s', 'of', 'lines', 'of', 'code.', 'Most', 'of', 'the', 'time', 'the', 'best', 'way', 'to', 'fight', 'complexity', 'is', 'by', 'not', 'creating', 'it', 'at', 'all.', 'Complexity', 'is', 'also', 'a', 'form', 'of', 'lock-in:', 'code', 'that', 'is', 'very', 'hard', 'to', 'understand', 'cannot', 'be', 'modified', 'by', 'users', 'in', 'an', 'independent', 'way', 'regardless', 'of', 'the', 'license.', 'One', 'of', 'the', 'main', 'Redis', 'goals', 'is', 'to', 'remain', 'understandable,', 'enough', 'for', 'a', 'single', 'programmer', 'to', 'have', 'a', 'clear', 'idea', 'of', 'how', 'it', 'works', 'in', 'detail', 'just', 'reading', 'the', 'source', 'code', 'for', 'a', 'couple', 'of', 'weeks.']
  i = 0
  env.cmd('ft.create', 'rof', 'ON', 'HASH', 'STOPWORDS', '0', 'schema', 'title', 'text', 'num', 'numeric')
  for i_a in range(len(a_list)):
    for i_b in range(len(b_list)):
      for i_c in range(len(c_list)):
        for i_d in range(len(d_list)):
          for i_e in range(len(e_list)):
            for i_f in range(len(f_list)):
              val = a_list[i_a] + ' ' + b_list[i_b] + ' ' + c_list[i_c] + ' ' + \
                    d_list[i_d] + ' ' + e_list[i_e] + ' ' + f_list[i_f]
              r.execute_command('ft.add', 'rof', 'doc{}'.format(i), 1.0, 'fields', 'title', val, 'num', i % 10)
              i+=1
              if i % 10000 == 0:
                r.execute()
              if i == q:
                r.execute()
                return

def testRoF(env):
  skipOnCrdtEnv(env)
  if env.cmd('info big') == '':
    env.skip()
  q = 100000
  createRdb(env, q)

  for _ in env.reloadingIterator():
    env.expect('ft.search rof * limit 0 0').equal([q])

