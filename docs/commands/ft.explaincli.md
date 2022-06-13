Returns the execution plan for a complex query but formatted for easier reading without using `redis-cli --raw`.

In the returned response, a `+` on a term is an indication of stemming.

#### Parameters

- **index**: The index name. The index must be first created with FT.CREATE
- **query**: The query string, as if sent to FT.SEARCH
- **DIALECT {dialect_version}**. Choose the dialect version to execute the query under. If not specified, the query will execute under the default dialect version set during module initial loading or via `FT.CONFIG SET` command.

@return

@array-reply with a string representation the execution plan.

@examples

```sh
$ redis-cli

127.0.0.1:6379> FT.EXPLAINCLI rd "(foo bar)|(hello world) @date:[100 200]|@date:[500 +inf]"
 1) INTERSECT {
 2)   UNION {
 3)     INTERSECT {
 4)       UNION {
 5)         foo
 6)         +foo(expanded)
 7)       }
 8)       UNION {
 9)         bar
10)         +bar(expanded)
11)       }
12)     }
13)     INTERSECT {
14)       UNION {
15)         hello
16)         +hello(expanded)
17)       }
18)       UNION {
19)         world
20)         +world(expanded)
21)       }
22)     }
23)   }
24)   UNION {
25)     NUMERIC {100.000000 <= @date <= 200.000000}
26)     NUMERIC {500.000000 <= @date <= inf}
27)   }
28) }
29)
```