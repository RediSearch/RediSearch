

template<class T>
void MinMaxHeap<T>::swap(int i, int j) {
  T tmp = at(i);
  at(i) = at(j);
  at(j) = tmp;
}

template<class T>
void MinMaxHeap<T>::bubbleup_min(int i) {
  if (i <= 0) return;

  int pp_idx = parent(parent(i));
  if (lt(i, pp_idx)) {
    swap(i, pp_idx);
    bubbleup_min(pp_idx);
  }
}

template<class T>
void MinMaxHeap<T>::bubbleup_max(int i) {
  if (i <= 0) return;

  int pp_idx = parent(parent(i));
  if (gt(i, pp_idx)) {
    swap(i, pp_idx);
    bubbleup_max(pp_idx);
  }
}

template<class T>
void MinMaxHeap<T>::bubbleup(int i) {
  if (i <= 0) return;

  int p_idx = parent(i);
  if (is_min(i)) {
    if (gt(i, p_idx)) {
      swap(i, p_idx);
      bubbleup_max(p_idx);
    } else {
      bubbleup_min(i);
    }
  } else {
    if (lt(i, p_idx)) {
      swap(i, p_idx);
      bubbleup_min(p_idx);
    } else {
      bubbleup_max(i);
    }
  }
}

template<class T>
int MinMaxHeap<T>::index_max_child_grandchild(int i) const {
  int a = first_child(i);
  int b = second_child(i);
  int d = second_child(a);
  int c = first_child(a);
  int f = second_child(b);
  int e = first_child(b);

  int min_idx = -1;
  if (a < heap_.size()) min_idx = a;
  if (b < heap_.size() && gt(b, min_idx)) min_idx = b;
  if (c < heap_.size() && gt(c, min_idx)) min_idx = c;
  if (d < heap_.size() && gt(d, min_idx)) min_idx = d;
  if (e < heap_.size() && gt(e, min_idx)) min_idx = e;
  if (f < heap_.size() && gt(f, min_idx)) min_idx = f;

  return min_idx;
}

template<class T>
int MinMaxHeap<T>::index_min_child_grandchild(int i) const {
  int a = first_child(i);
  int b = second_child(i);
  int c = first_child(a);
  int d = second_child(a);
  int e = first_child(b);
  int f = second_child(b);

  int min_idx = -1;
  if (a < heap_.size()) min_idx = a;
  if (b < heap_.size() && lt(b, min_idx)) min_idx = b;
  if (c < heap_.size() && lt(c, min_idx)) min_idx = c;
  if (d < heap_.size() && lt(d, min_idx)) min_idx = d;
  if (e < heap_.size() && lt(e, min_idx)) min_idx = e;
  if (f < heap_.size() && lt(f, min_idx)) min_idx = f;

  return min_idx;
}

template<class T>
void MinMaxHeap<T>::trickledown_max(int i) {
  int m = index_max_child_grandchild(i);
  if (m <= -1) return;
  if (m > second_child(i)) {
    // m is a grandchild
    if (gt(m, i)) {
      swap(i, m);
      if (lt(m, parent(m))) {
        swap(m, parent(m));
      }
      trickledown_max(m);
    }
  } else {
    // m is a child
    if (gt(m, i)) swap(i, m);
  }
}

template<class T>
void MinMaxHeap<T>::trickledown_min(int i) {
  int m = index_min_child_grandchild(i);
  if (m <= -1) return;
  if (m > second_child(i)) {
    // m is a grandchild
    if (lt(m, i)) {
      swap(i, m);
      if (gt(m, parent(m))) {
        swap(m, parent(m));
      }
      trickledown_min(m);
    }
  } else {
    // m is a child
    if (lt(m, i)) swap(i, m);
  }
}

template<class T>
void MinMaxHeap<T>::trickledown(int i) {
  if (is_min(i)) {
    trickledown_min(i);
  } else {
    trickledown_max(i);
  }
}

template<class T>
void MinMaxHeap<T>::insert(T value) {
  heap_.push_back(value);
  bubbleup(heap_.size() - 1);
}

template<class T>
T MinMaxHeap<T>::pop_min() {
  if (size() > 1) {
    T d = at(0);
    at(0) = heap_.back();
    heap_.pop_back();
    trickledown(1);
    return d;
  }

  if (size() == 1) {
    return at(0);
  }
}

template<class T>
T MinMaxHeap<T>::pop_max() {
  if (size() > 2) {
    int idx = lt(1, 2) ? 2 : 1;

    T d = at(idx);
    at(idx) = heap_.back();
    heap_.pop_back();
    trickledown(idx);
    return d;
  }

  if (heap_.size() == 2) {
    return at(1);
  }

  if (heap_.size() == 1) {
    return at(0);
  }
}

template<class T>
const T& MinMaxHeap<T>::peek_min() const {
  if (!heap_.empty()) {
    return at(1);
  }
}

template<class T>
const T& MinMaxHeap<T>::peek_max() const {
  if (heap_.size() > 2) {
    return max(1, 2);
  }
  if (heap_.size() == 2) {
    return at(1);
  }
  if (heap_.size() == 1) {
    return at(0);
  }
}
