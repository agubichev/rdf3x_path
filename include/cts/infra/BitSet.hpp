#ifndef H_cts_infra_BitSet
#define H_cts_infra_BitSet
//---------------------------------------------------------------------------
/// A bit set used for representating partial optimization problems. The
/// current implementation has a limited maximum width, if desired a dynamic
/// bit set can be implemented instead.
class BitSet
{
   public:
   /// The type of the value
   typedef unsigned long value_t;
   /// The maximum width of the bit set representation.
   static const unsigned maxWidth = sizeof(value_t)*8;

   private:
   /// The first bit
   static const value_t one = 1;

   /// The value
   value_t value;

   /// Constructor
   explicit BitSet(value_t value) : value(value) {}

   public:
   /// Constructor
   BitSet() : value(0) {}

   /// Set a specific entry
   void set(unsigned i) { value|=one<<i; }
   /// Clear a specific entry
   void clear(unsigned i) { value&=~(one<<i); }
   /// Test a specific entry
   bool test(unsigned i) const { return value&(one<<i); }

   /// Equal
   bool operator==(const BitSet& o) const { return value==o.value; }
   /// Not equal?
   bool operator!=(const BitSet& o) const { return value!=o.value; }
   /// Compare for set operators
   bool operator<(const BitSet& o) const { return value<o.value; }

   /// Subset or equal?
   bool subsetOf(const BitSet& o) const { return (value&o.value)==value; }
   /// Overlap?
   bool overlapsWith(const BitSet& o) const { return value&o.value; }

   /// Union
   BitSet unionWith(const BitSet& o) const { return BitSet(value|o.value); }
   /// Difference
   BitSet differenceWith(const BitSet& o) const { return BitSet(value&(~o.value)); }
   /// Intersection
   BitSet intersectWith(const BitSet& o) const { return BitSet(value&o.value); }
};
//---------------------------------------------------------------------------
#endif
