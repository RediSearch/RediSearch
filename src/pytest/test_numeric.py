from RLTest import Env

def testNumericAccuracy(env):
  repeat = 50
  decimal_list = [1.1, 3.63, 7.987654321, -3.14, -1000434, 654]

  for decimal in decimal_list:
    env.expect('ft.create idx SCHEMA f1 NUMERIC').ok()
    for ii in range(50):
      env.expect('ft.add idx doc%d 1.0 FIELDS f1 %r' % (ii, ii * decimal)).ok()

    # test range of exact value returns value
    for ii in range(50):
      res = env.cmd('ft.search idx', '@f1:[%r %r]' % (ii * decimal, ii * decimal))
      #env.assertEqual(ii, ii + 1)
      #env.assertNotEqual(res, [0L])
      if len(res) == 3:
        env.assertEqual(float(res[2][1]), ii * decimal)
      else:
        env.assertTrue(False)
    
    for ii in range(50):
      range1 = 0 if decimal > 0 else ii * decimal
      range2 = ii * decimal if decimal > 0 else 0
      # test all values in range return - inclusive
      res = env.cmd('ft.search idx', '@f1:[%r %r]' % (range1, range2), 'limit', 0, 0)
      env.assertEqual(res[0], ii + 1)

      # test all values in range return - exclusive
      res = env.cmd('ft.search idx', '@f1:[(%r (%r]' % (range1, range2), 'limit', 0, 0)
      env.assertEqual(res[0], max(ii - 1, 0))
    
    env.expect('ft.drop idx').ok()
