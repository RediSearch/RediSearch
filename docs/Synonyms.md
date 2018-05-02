# Synonyms Support

## Overview
RediSearch supports synonyms - that is searching for synonyms words defined by the synonym data structure.

The synonym data structure is a set of groups, each group contains synonym terms.
for example: the following synonym data structure
contains three groups, each group contains three synonym terms:

```
{boy, child, baby}
{girl, child, baby}
{man, person, adult}
```
When those three groups are located inside the synonym data structure, it is possible to search for 'child' and receive documents
contains 'boy', 'girl', 'child' and 'baby'.

## Commands
### Add synonym group
The following command is used to create a new synonyms group:
```
FT.SYNADD <index name> <term1> <term2> ...
```
The command returns the synonym group id which can later be used to add additional terms to that synonym group.
Only documents which was indexed after the adding operation will be effected.

### Update synonym group
The following command is used to update an existing synonym group with additional terms:
```
FT.SYNUPDATE <index name> <synonym group id> <term1> <term2> ...
```
Only documents which was indexed after the update will be effected.

### Dump synonyms data structure
The following command is used to dump the synonyms data structure:
```
FT.SYNDUMP <index name>
```
Returns a list of synonym terms and their synonym group ids.

## Enabling synonyms on search
By default synonyms are not enabled when searching. In order to enable it add the EXPENDER argument to the search command with value SYN
```
FT.SEARCH <index> <query> EXPANDER SYN
```