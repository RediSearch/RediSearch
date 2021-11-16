# CmdParse - Complex Redis Command Parsing

This is an API to help with parsing complex redis commands - where you need complex and recursive command structues. It can validate and semantically parse commands into a structured AST of sorts.

The main idea is that you can define a Schema for the command, detailing its structure, argument types and so on - and the API can automatically validate and parse incoming redis arguments. 

It currently supports:

* Named and positional arguments.
* Required and optional arguments.
* Typed parsing of strings, doubles and integers.
* Argument tuples (with typed parsing) (e.g. `LIMIT {min:int} {max:int}`)
* Argument vectors (e.g. `KEYS 3 foo bar baz`)
* Flags (e.g. `SET [NX]`)
* Options (`SET [XX|NX]`)
* Nested sub-commands (`GROUPBY foo REDUCE SUM foo REDUCE AVG bar`)
* Variadic vectors at the end of the argument list (as seen on `MSET`)
* Any combination of the above.
* Ouputting (currently pretty crude) help documentation from the schema.

## Quick Example

Let's define the command `ZADD key [NX|XX] [CH] [INCR] score member [score member ...]`

Defining the schema would be expressed as:

```c
  // Creating the command
  CmdSchemaNode *sc = NewSchema("ZADD", "ZAdd command");

  // Adding the key argument - string typed.
  // Note that even positional args need a name to be referenced by
  CmdSchema_AddPostional(sc, "key", CmdSchema_NewArg('s'), CmdSchema_Required);

  // Adding [NX|XX]
  CmdSchema_AddPostional(sc, "nx_xx", CmdSchema_NewOption(2, (const char *[]){"NX", "XX"}),
                         CmdSchema_Optional);
  // Add the CH and INCR flags
  CmdSchema_AddFlag(sc, "CH");
  CmdSchema_AddFlag(sc, "INCR");

  // Add the score/member variadic vector. "ds" means pairs will be consumed as double and string
  // and grouped into arrays
  CmdSchema_AddPostional(sc, "pairs", CmdSchema_NewVariadicVector("ds"), CmdSchema_Required);

```

And parsing it is done by calling `CmdParser_ParseRedisModuleCmd`, giving it the schema and the command arguments. 

Assuming our argument list is `"ZADD", "foo", "NX", "0", "bar", "1.3", "baz", "5", "froo"`, we will do the following:

```c

int MyCmd(RedisModuleCtx *ctx, int argc, RedisModuleString **argv) {
  char *err;
  CmdArg *cmd;
  // Parse the command
  if (CmdParser_ParseRedisModuleCmd(sc, &cmd, argv, argc, &err, 1) == CMDPARSE_ERR) {
    RedisModule_ReplyWithError(ctx, err);

    // if an error is returned, we need to free it
    free(err);
    
    return REDISMODULE_ERR;
  }

  // just debug printing
  CmdArg_Print(cmd, 0);

  /* handle the command */
  ...
}
```

This will print the parsed command tree:

```
{
  key: =>  "foo"
  nx_xx: =>  "NX"
  pairs: =>  [[0.000000,"bar"],[1.300000,"baz"],[5.000000,"froo"]]
  CH: =>  FALSE
  INCR: =>  FALSE
}
```

The CmdArg object generated from the parsing resembles a JSON object tree, and is combined of a union of:

* Objects (key/value pairs, non unique)
* Arrays
* Doubles
* Integers
* Flags (booleans)
* Strings

We have a few convenience functions to iterate the arguments, for example, walking over the score/member pairs in the above command would look like:

```c
  // Get the score/member vector
  CmdArg *pairs = CmdArg_FirstOf(cmd, "pairs");
  // Create an iterator for the score/member pairs
  CmdArgIterator it = CmdArg_Children(pairs);
  CmdArg *pair;
  // Walk the iterator
  while (NULL != (pair = CmdArgIterator_Next(&it))) {

    // Accessing the sub elements is done in a similar way. Each element is an array in turn. Since
    // we know its size and it is typed, we can access the values directly
    printf("Score: %f, element %s\n", CMDARRAY_ELEMENT(pair, 0)->d,
           CMDARRAY_ELEMENT(pair, 1)->s.str);
  }
```

