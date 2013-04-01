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
#ifndef FERRARI_INTERVALLIST_H_
#define FERRARI_INTERVALLIST_H_
//-------------------------------------------------------------------------------------------------
#include <functional>
#include <iostream>
#include <vector>
#include <set>
#include <string>
//-------------------------------------------------------------------------------------------------
template <typename T>
struct second_smaller {
  bool operator()(const T& x, const T& y) const {
    return x.second < y.second;
  }
};
//-------------------------------------------------------------------------------------------------
class IntervalList {
private:
  // interval endpoints
  std::vector<unsigned> lower_;
  std::vector<unsigned> upper_;
  std::vector<char> exact_;

public:
  enum containment {
    NOT = -1, MAYBE = 0, YES = 1
  };

  IntervalList();

  IntervalList(unsigned a, unsigned b, char ex = 1);

  ~IntervalList();

  // modifications
  void merge_exact(const IntervalList& other);
  void merge(const IntervalList& other);
  void add(unsigned a, unsigned b, char ex=1);
  void restrict(const unsigned& k);

  inline void append(unsigned x) {
    if (!empty() && upper_.back() == x - 1) {
      upper_.back() = x;
    } else {
      lower_.push_back(x);
      upper_.push_back(x);
      exact_.push_back(1);
    }
  }

  // retrieval
  int find(const unsigned& x, int min=0) const;
  containment contains(const unsigned& x) const;

  // accessors
  inline unsigned min() const {
    return lower_.front();
  }

  inline unsigned max() const {
    return upper_.back();
  }

  inline bool empty() const {
    return lower_.empty();
  }

  inline unsigned size() const {
    return lower_.size();
  }

  inline unsigned count_approximate() const {
    unsigned count = 0;
    for (unsigned i = 0; i < lower_.size(); ++i) {
      if (exact_[i] == 0) {
        count += interval_length(i);
      }
    }
    return count;
  }

  inline unsigned count_exact() const {
    unsigned count = 0;
    for (unsigned i = 0; i < lower_.size(); ++i) {
      if (exact_[i]) {
        count += interval_length(i);
      }
    }
    return count;
  }

  inline const std::vector<unsigned>& get_lower() const {
    return lower_;
  }

  inline const std::vector<unsigned>& get_upper() const {
    return upper_;
  }

  inline const std::vector<char>& get_exact() const {
    return exact_;
  }

  inline unsigned interval_length(const unsigned& index) const {
    return upper_[index] - lower_[index] + 1;
  }

  inline unsigned gap_length(const unsigned& index) const {
    return lower_[index + 1] - upper_[index] - 1;
  }

  friend std::ostream& operator<<(std::ostream& out, const IntervalList &i);
};
//-------------------------------------------------------------------------------------------------
#endif /* INTERVALLIST_H_ */
