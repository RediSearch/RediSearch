#define is_min(n) ((log2_32(n) & 1) == 0)
#define parent(n) (n / 2)
#define first_child(n) (n * 2)
#define second_child(n) ((n * 2) + 1)

static const int tab32[32] = {0, 9,  1,  10, 13, 21, 2,  29, 11, 14, 16, 18, 22, 25, 3, 30,
                              8, 12, 20, 28, 15, 17, 24, 7,  19, 27, 23, 6,  26, 5,  4, 31};

static inline int log2_32(uint32_t value) {
  value |= value >> 1;
  value |= value >> 2;
  value |= value >> 4;
  value |= value >> 8;
  value |= value >> 16;
  return tab32[(uint32_t)(value * 0x07C4ACDD) >> 27];
}

template<class T>
void MinMaxHeap<T>::_swap(int i, int j) {
  T tmp = at(i);
  at(i) = at(j);
  at(j) = tmp;
}

template<class T>
void MinMaxHeap<T>::_bubbleup_min(int i) {
  int pp_idx = parent(parent(i));
  if (pp_idx <= 0) return;

  if (_lt(i, pp_idx)) {
    _swap(i, pp_idx);
    _bubbleup_min(pp_idx);
  }
}

template<class T>
void MinMaxHeap<T>::_bubbleup_max(int i) {
  int pp_idx = parent(parent(i));
  if (pp_idx <= 0) return;

  if (_gt(i, pp_idx)) {
    _swap(i, pp_idx);
    _bubbleup_max(pp_idx);
  }
}

template<class T>
void MinMaxHeap<T>::_bubbleup(int i) {
  int p_idx = parent(i);
  if (p_idx <= 0) return;

  if (is_min(i)) {
    if (_gt(i, p_idx)) {
      _swap(i, p_idx);
      _bubbleup_max(p_idx);
    } else {
      _bubbleup_min(i);
    }
  } else {
    if (_lt(i, p_idx)) {
      _swap(i, p_idx);
      _bubbleup_min(p_idx);
    } else {
      _bubbleup_max(i);
    }
  }
}

template<class T>
int MinMaxHeap<T>::_index_max_child_grandchild(int i) {
  int a = first_child(i);
  int b = second_child(i);
  int d = second_child(a);
  int c = first_child(a);
  int f = second_child(b);
  int e = first_child(b);

  int min_idx = -1;
  if (a <= size()) min_idx = a;
  if (b <= size() && _gt(b, min_idx)) min_idx = b;
  if (c <= size() && _gt(c, min_idx)) min_idx = c;
  if (d <= size() && _gt(d, min_idx)) min_idx = d;
  if (e <= size() && _gt(e, min_idx)) min_idx = e;
  if (f <= size() && _gt(f, min_idx)) min_idx = f;

  return min_idx;
}

template<class T>
int MinMaxHeap<T>::_index_min_child_grandchild(int i) {
  int a = first_child(i);
  int b = second_child(i);
  int c = first_child(a);
  int d = second_child(a);
  int e = first_child(b);
  int f = second_child(b);

  int min_idx = -1;
  if (a <= size()) min_idx = a;
  if (b <= size() && _lt(b, min_idx)) min_idx = b;
  if (c <= size() && _lt(c, min_idx)) min_idx = c;
  if (d <= size() && _lt(d, min_idx)) min_idx = d;
  if (e <= size() && _lt(e, min_idx)) min_idx = e;
  if (f <= size() && _lt(f, min_idx)) min_idx = f;

  return min_idx;
}

template<class T>
void MinMaxHeap<T>::_trickledown_max(int i) {
  int m = _index_max_child_grandchild(i);
  if (m <= -1) return;
  if (m > second_child(i)) {
    // m is a grandchild
    if (_gt(m, i)) {
      _swap(i, m);
      if (_lt(m, parent(m))) {
        _swap(m, parent(m));
      }
      _trickledown_max(m);
    }
  } else {
    // m is a child
    if (_gt(m, i)) _swap(i, m);
  }
}

template<class T>
void MinMaxHeap<T>::_trickledown_min(int i) {
  int m = _index_min_child_grandchild(i);
  if (m <= -1) return;
  if (m > second_child(i)) {
    // m is a grandchild
    if (_lt(m, i)) {
      _swap(i, m);
      if (_gt(m, parent(m))) {
        _swap(m, parent(m));
      }
      _trickledown_min(m);
    }
  } else {
    // m is a child
    if (_lt(m, i)) _swap(i, m);
  }
}

template<class T>
void MinMaxHeap<T>::_trickledown(int i) {
  if (is_min(i)) {
    _trickledown_min(i);
  } else {
    _trickledown_max(i);
  }
}

template<class T>
void MinMaxHeap<T>::insert(T value) {
  push_back(value);
  _bubbleup(size());
}

template<class T>
T MinMaxHeap<T>::pop_min() {
  if (size() > 1) {
    T d = at(1);
    at(1) = this->back();
    pop_back();
    _trickledown(1);
    return d;
  }

  if (size() == 1) {
    return at(1);
  }

  return NULL;
}

template<class T>
T MinMaxHeap<T>::pop_max() {
  if (size() > 2) {
    int idx = 2;
    if (_lt(2, 3)) idx = 3;

    T d = at(idx);
    at(idx) = this->back();
    pop_back();
    _trickledown(idx);
    return d;
  }

  if (size() == 2) {
    return at(2);
  }

  if (size() == 1) {
    return at(1);
  }

  return NULL;
}

template<class T>
T MinMaxHeap<T>::peek_min() const {
  if (!empty()) {
    return at(0);
  }
  return NULL;
}

template<class T>
T MinMaxHeap<T>::peek_max() const {
  if (size() > 2) {
    return _max(2, 3);  // h->data[2], h->data[3]);
  }
  if (size() == 2) {
    return at(2);
  }
  if (size() == 1) {
    return at(1);
  }
  return NULL;
}
