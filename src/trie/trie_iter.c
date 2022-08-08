#include "trie.h"
#include "query_error.h"

///////////////////////////////////////////////////////////////////////////////////////////////

// Push a new trie node on the iterator's stack

void TrieIterator::Push(TrieNode *node, int skipped) {
  stack.emplace_back(StackNode{skipped, node});
}

//---------------------------------------------------------------------------------------------

// pop a node from the iterator's stcak

void TrieIterator::Pop() {
  if (stack.empty()) return;

  StackNode &curr = current();
  dfafilter.StackPop(curr.stringOffset);

  if (runes.len() >= curr.stringOffset) {
    throw Error("Invalid iterator state");
  }
  runes.pop(curr.stringOffset);
  stack.pop_back();
}

//---------------------------------------------------------------------------------------------

// Single step iteration, feeding the given filter/automaton with the next character

TrieIterator::StepResult TrieIterator::Step(int *match) {
  if (stack.empty()) {
    return __STEP_STOP;
  }

  StackNode &curr = current();
  int matched = 0;
  switch (curr.state) {
    case ITERSTATE_MATCH:
      Pop();
      goto next;

    case ITERSTATE_SELF:
      if (curr.stringOffset < curr.n->_len) {
        // get the current rune to feed the filter
        rune b = curr.n->_runes[curr.stringOffset];

        // run the next character in the filter
        FilterCode rc = dfafilter.Filter(b, &matched, match);

        // if we should stop...
        if (rc == F_STOP) {
          // match stop - change the state to MATCH and return
          if (matched) {
            curr.state = ITERSTATE_MATCH;
            return __STEP_MATCH;
          }
          // normal stop - just pop and continue
          Pop();
          goto next;
        }

        // advance the buffer offset and character offset
        runes.append(b);
        curr.stringOffset++;

        // if we don't have a filter, a "match" is when we reach the end of the
        // node
        if (!filter) {
          if (curr.n->_len > 0 && curr.stringOffset == curr.n->_len &&
              curr.n->isTerminal() && !curr.n->isDeleted()) {
            matched = 1;
          }
        }

        return matched ? __STEP_MATCH : __STEP_CONT;
      } else {
        // switch to "children mode"
        curr.state = ITERSTATE_CHILDREN;
      }

    case ITERSTATE_CHILDREN:
    default:
      if (curr.n->_sortmode != TRIENODE_SORTED_SCORE) {
        curr.n->sortChildren();
      }
      // push the next child
      if (curr.childOffset < curr.n->_children.size()) {
        TrieNode *ch = curr.n->_children[curr.childOffset++];
        if (ch->_maxChildScore >= minScore || ch->_score >= minScore) {
          Push(ch, 0);
          nodesConsumed++;
        } else {
          //Push(ch, 1);
          nodesSkipped++;
        }
      } else {
        // at the end of the node - pop and go up
        Pop();
      }
  }

next:
  return __STEP_CONT;
}

//---------------------------------------------------------------------------------------------

// Iterate the tree with a step filter, which tells the iterator whether to continue down the trie
// or not. This can be a levenshtein automaton, a regex automaton, etc.
// NULL filter means just continue iterating the entire trie. ctx is the filter's context.

TrieIterator TrieNode::Iterate(StepFilter f, StackPopCallback pf, DFAFilter *filter) {
  return TrieIterator{this, f, pf, filter};
}

//---------------------------------------------------------------------------------------------

TrieIterator::TrieIterator(DFAFilter *filter) :
    filter(NULL), popCallback(NULL), minScore(0), dfafilter(*filter) {
}

//---------------------------------------------------------------------------------------------

TrieIterator::TrieIterator(TrieNode *node, StepFilter f, StackPopCallback pf, DFAFilter *filter) :
    filter(f), popCallback(pf), minScore(0), dfafilter(*filter) {
  Push(node, false);
}

//---------------------------------------------------------------------------------------------

// Iterate to the next matching entry in the trie.
// Returns true if we can continue, or false if we're done and should exit.

bool TrieIterator::Next(Runes &ret_runes, RSPayload &payload, float &score, void *match) {
  StepResult rc;
  while ((rc = Step(match)) != __STEP_STOP) {
    if (rc == __STEP_MATCH) {
      StackNode &sn = current();

      if (sn.n->isTerminal() && sn.n->_len == sn.stringOffset && !sn.n->isDeleted()) {
        ret_runes = runes;
        score = sn.n->_score;
        if (sn.n->_payload != NULL) {
          payload = RSPayload(sn.n->_payload);
        } else {
          payload.reset();
        }
        return true;
      }
    }
  }

  return false;
}

///////////////////////////////////////////////////////////////////////////////////////////////
