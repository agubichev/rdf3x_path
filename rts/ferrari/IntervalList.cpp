//--------------------------------------------------------------------------------------------------
// Ferrari Reachability Index
// (c) 2012 Stephan Seufert. Web site: http://www.mpi-inf.mpg.de/~sseufert
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//--------------------------------------------------------------------------------------------------
#include "rts/ferrari/IntervalList.hpp"
//--------------------------------------------------------------------------------------------------
#include <algorithm>
#include <assert.h>
#include <cstdio>
//-------------------------------------------------------------------------------------------------
IntervalList::IntervalList() {
}
//-------------------------------------------------------------------------------------------------
IntervalList::IntervalList(unsigned a, unsigned b, char ex) {
  lower_.push_back(a);
  upper_.push_back(b);
  exact_.push_back(ex);
}
//-------------------------------------------------------------------------------------------------
IntervalList::~IntervalList() {
}
//-------------------------------------------------------------------------------------------------
void IntervalList::merge_exact(const IntervalList& other) {
  // find starting point
  if (other.empty()) {
    return;
  }
  int last_index = 0;

  const std::vector<unsigned>& o_lower = other.get_lower();
  const std::vector<unsigned>& o_upper = other.get_upper();
  if (empty()) {
    std::copy(o_lower.begin(), o_lower.end(), std::back_inserter(lower_));
    std::copy(o_upper.begin(), o_upper.end(), std::back_inserter(upper_));
    return;
  } else {
    // find relevant position in interval list
    unsigned a, b;
    int idxa, idxb;
    bool insa, insb;
    for (unsigned i = 0; i < o_lower.size(); ++i) {
      a = o_lower[i], b = o_upper[i];
      insa = false, insb = false;
      idxa = find(a); //, last_index_);
      (void) last_index;
      idxb = idxa + 1;

      /* distinguish 4 cases a,b between/within intervals */
      // determine
      //    - index of interval containing or left of a, flag: inside
      //    - index of interval containing or right of b, flag: inside
      if (idxa != -1 && a <= upper_[idxa] + 1) {
        insa = true;
      }
      while (idxb < int(lower_.size()) && b >= lower_[idxb] - 1) {
        ++idxb;
      }

      if (idxb != 0 && b <= upper_[idxb - 1] + 1) {
        insb = true;
        --idxb;
      } else {
        insb = false;
      }

      // special cases
      if (!insb && idxb == 0) {
        // before everything
        lower_.insert(lower_.begin(), a);
        upper_.insert(upper_.begin(), b);
        last_index = 0;
        continue;                 // done
      } else if (!insa && idxa == int(lower_.size()) - 1) {
        // after everything
        lower_.push_back(a);
        upper_.push_back(b);
        last_index = lower_.size() - 1;
        continue;                 // done
      }
      if (!insa) {
        ++idxa;
      }
      if (!insb) {
        --idxb;
      }

      if (idxa > idxb) {
        // new interval between existing
        //assert(int(lower_.size()) > idxa);
        lower_.insert(lower_.begin() + idxa, a);
        upper_.insert(upper_.begin() + idxa, b);
        last_index = idxa;
        continue;                 // done
      }

      // update interval boundaries
      lower_[idxa] = std::min(lower_[idxa], a);
      upper_[idxb] = std::max(upper_[idxb], b);

      // general case
      lower_.erase(lower_.begin() + idxa + 1, lower_.begin() + idxb + 1);
      upper_.erase(upper_.begin() + idxa, upper_.begin() + idxb);
      last_index = idxb - idxa - 1;
    }
  }
}
//-------------------------------------------------------------------------------------------------
void IntervalList::merge(const IntervalList& other) {
  // find starting point
  if (other.empty()) {
    return;
  }
  int last_index = 0;

  const std::vector<unsigned>& o_lower = other.get_lower();
  const std::vector<unsigned>& o_upper = other.get_upper();
  const std::vector<char>& o_exact = other.get_exact();
  //assert((o_lower.size() == o_upper.size()) && (o_lower.size() == o_exact.size()));
  if (empty()) {
    std::copy(o_lower.begin(), o_lower.end(), std::back_inserter(lower_));
    std::copy(o_upper.begin(), o_upper.end(), std::back_inserter(upper_));
    std::copy(o_exact.begin(), o_exact.end(), std::back_inserter(exact_));
    return;
  } else {
    // find relevant position in interval list
    unsigned a, b;
    char ex;
    int idxa, idxb;
    bool insa, insb;
    for (unsigned i = 0; i < o_lower.size(); ++i) {
      a = o_lower[i], b = o_upper[i];
      ex = o_exact[i];
      insa = false, insb = false;

      // determine idxa
      if (a < lower_.front()) {
        idxa = -1;
      } else if (a > upper_.back()) {
        idxa = upper_.size() - 1;
      } else {
        // no special case, perform binary search
        int _min = last_index, _max = lower_.size(), _mid;
        while (true) {
          if (_min == _max) {
            if (a < lower_[_min]) {
              idxa = _min - 1; // immediately to the left
              break;
            } else {
              idxa = _min; // included in interval, or immediately to the right
              break;
            }
          }
          _mid = (_max + _min) / 2;
          if (lower_[_mid] <= a && a <= upper_[_mid]) {
            idxa = _mid;
            break;
          } else if (a < lower_[_mid]) {
            _max = _mid;
          } else {
            _min = _mid + 1;
          }
        }
      }
      idxb = idxa + 1;

      /* distinguish 4 cases a,b between/within intervals */
      // determine
      //    - index of interval containing or left of a, flag: inside
      //    - index of interval containing or right of b, flag: inside
      if (idxa != -1 && a <= upper_[idxa] + 1) {
        insa = true;
      }
      while (idxb < int(lower_.size()) && b >= lower_[idxb] - 1) {
        ++idxb;
      }

      if (idxb != 0 && b <= upper_[idxb - 1] + 1) {
        insb = true;
        --idxb;
      } else {
        insb = false;
      }

      // special cases
      if (!insb && idxb == 0) {
        // before everything
        lower_.insert(lower_.begin(), a);
        upper_.insert(upper_.begin(), b);
        exact_.insert(exact_.begin(), ex);
        last_index = 0;
        continue;                 // done
      } else if (!insa && idxa == int(lower_.size()) - 1) {
        // after everything
        lower_.push_back(a);
        upper_.push_back(b);
        exact_.push_back(ex);
        last_index = lower_.size() - 1;
        continue;                 // done
      }

      if (!insa) {
        ++idxa;
      }
      if (!insb) {
        --idxb;
      }

      if (idxa > idxb) {
        // new interval between existing
        //assert(int(lower_.size()) > idxa);
        lower_.insert(lower_.begin() + idxa, a);
        upper_.insert(upper_.begin() + idxa, b);
        exact_.insert(exact_.begin() + idxa, ex);
        last_index = idxa;
        continue;                 // done
      }

      // non-exact interval subsumed
      if (!ex && insa && insb && idxa == idxb && lower_[idxa] <= a
          && b <= upper_[idxa]) {
        last_index = idxa;
        continue;
      }

      // update interval boundaries
      lower_[idxa] = std::min(lower_[idxa], a);
      upper_[idxb] = std::max(upper_[idxb], b);

      // general case
      assert(idxb <= int(lower_.size()));
      lower_.erase(lower_.begin() + idxa + 1, lower_.begin() + idxb + 1);
      upper_.erase(upper_.begin() + idxa, upper_.begin() + idxb);
      char new_exact = ex;
      if (ex) {
        for (int i = idxa; i <= idxb; ++i) {
          if (!exact_[i]) {
            new_exact = 0;
            break;
          }
        }
      }
      exact_[idxa] = new_exact;
      exact_.erase(exact_.begin() + idxa + 1, exact_.begin() + idxb + 1);
      last_index = std::max(0, idxa);
    }
  }
}
//-------------------------------------------------------------------------------------------------
void IntervalList::add(unsigned a, unsigned b, char ex) {
  //assert(empty());
  lower_.push_back(a);
  upper_.push_back(b);
  exact_.push_back(ex);
}
//-------------------------------------------------------------------------------------------------
int IntervalList::find(const unsigned& x, int min) const {
  if (empty() || x < lower_.front()) {
    return -1;
  } else if (x > upper_.back()) {
    return upper_.size() - 1;
  }

  // no special case, perform binary search
  int _min = std::max(0, min), _max = lower_.size(), _mid;
  while (true) {
    if (_min == _max) {
      if (x < lower_[_min]) {
        return _min - 1; // immediately to the left
      } else {
        return _min; // included in interval, or immediately to the right
      }
    }
    _mid = (_max + _min) / 2;
    if (lower_[_mid] <= x && x <= upper_[_mid]) {
      return _mid;
    } else if (x < lower_[_mid]) {
      _max = _mid;
    } else {
      _min = _mid + 1;
    }
  }
  return -1;
}
//-------------------------------------------------------------------------------------------------
IntervalList::containment IntervalList::contains(const unsigned& x) const {
  if (x < lower_.front() || x > upper_.back()) {
    return IntervalList::NOT;
  }
  int _min = 0, _max = lower_.size(), _mid;
  while (true) {
    if (_min == _max) {
      if (lower_[_min] <= x && x <= upper_[_min]) {
        if (exact_[_min])
          return IntervalList::YES;
        else
          return IntervalList::MAYBE;
      } else
        return IntervalList::NOT;
    }
    _mid = (_max + _min) / 2;
    if (lower_[_mid] <= x && x <= upper_[_mid]) {
      if (exact_[_mid])
        return IntervalList::YES;
      else
        return IntervalList::MAYBE;
    } else if (x < lower_[_mid]) {
      _max = _mid;
    } else {
      _min = _mid + 1;
    }
  }
  return IntervalList::NOT; // never reached
}
//-------------------------------------------------------------------------------------------------
void IntervalList::restrict(const unsigned& k) {
  // already done?
  if (lower_.size() <= k)
    return;

  std::vector<std::pair<unsigned, unsigned> > gaps;
  for (unsigned i = 0; i < lower_.size() - 1; ++i) {
    if (i == 0 && exact_.front()) {
      gaps.push_back(std::make_pair(0, lower_[1] - lower_[0]));
    } else if (i == lower_.size() - 1 && exact_.back()) {
      gaps.push_back(std::make_pair(i, upper_[i] - upper_[i - 1]));
    } else {
      gaps.push_back(std::make_pair(i, gap_length(i)));
    }
  }
  std::make_heap(gaps.begin(), gaps.end(),
      second_smaller<std::pair<unsigned, unsigned> >());
  std::vector<char> selected(lower_.size() - 1, 0);

  unsigned id, selected_gaps = 0;
  while (!gaps.empty() && selected_gaps < k - 1) {

    id = gaps.front().first;
    std::pop_heap(gaps.begin(), gaps.end(),
        second_smaller<std::pair<unsigned, unsigned> >());
    gaps.pop_back();

    if (selected[id]) {
      continue;
    }

    ++selected_gaps;
    selected[id] = 1;

    /* Schema of indices of intervals and gaps:
     * list of N intervals:
     * [  I0  ]  g0  [  I1  ]  g1  [  I2  ]  g2  ... gN-2  [  IN-1  ]
     */
    if (id && selected[id - 1] == 0) {
      gaps.push_back(
          std::make_pair(id - 1,
              gap_length(id - 1) + interval_length(id)
                         + (id >= 2 ? selected[id - 2] * interval_length(id - 1)
                            : interval_length(id - 1))));
      std::push_heap(gaps.begin(), gaps.end());
    }
    
    if (id < lower_.size() - 2 && selected[id + 1] == 0) {
      gaps.push_back(
          std::make_pair(id + 1,
              gap_length(id + 1) + interval_length(id + 1)
                  + (id + 2 == lower_.size() - 1 ? interval_length(id + 2) :
                      selected[id + 2] * interval_length(id + 2))));
      std::push_heap(gaps.begin(), gaps.end());
    }
  }

  std::vector<unsigned> n_lower, n_upper;
  std::vector<char> n_exact;
  n_lower.push_back(lower_.front());
  bool prev_gap_selected = true;
  for (unsigned i = 0; i < lower_.size() - 1; ++i) {
    if (selected[i]) {
      n_upper.push_back(upper_[i]);
      n_lower.push_back(lower_[i + 1]);
      n_exact.push_back(prev_gap_selected && exact_[i] == 1);
      prev_gap_selected = true;
    } else {
      prev_gap_selected = false;
    }
  }
  n_upper.push_back(upper_.back());
  n_exact.push_back(prev_gap_selected && exact_.back() == 1);

  lower_ = n_lower;
  upper_ = n_upper;
  exact_ = n_exact;
  assert(lower_.size() == exact_.size());
}
//-------------------------------------------------------------------------------------------------
std::ostream& operator<<(std::ostream& out, const IntervalList &il) {
  out << "{";
  for (unsigned i = 0; i < il.size(); ++i) {
    if (il.exact_[i])
      out << " [" << il.lower_[i] << "," << il.upper_[i] << "]";
    else
      out << " (" << il.lower_[i] << "," << il.upper_[i] << ")";
  }
  out << " }";
  return out;
}
//-------------------------------------------------------------------------------------------------
