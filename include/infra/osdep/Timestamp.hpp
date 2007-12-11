#ifndef H_infra_osdep_Timestamp
#define H_infra_osdep_Timestamp
//---------------------------------------------------------------------------
class AvgTime;
//---------------------------------------------------------------------------
/// A high resolution timestamp
class Timestamp
{
   private:
   /// The data
   char data[64];

   friend class AvgTime;

   public:
   /// Constructor
   Timestamp();

   /// Hash
   unsigned long long getHash() const { return *reinterpret_cast<const unsigned long long*>(data); }

   /// Difference in ms
   unsigned operator-(const Timestamp& other) const;
};
//---------------------------------------------------------------------------
/// Aggregate
class AvgTime {
   private:
   /// The data
   char data[64];
   /// Count
   unsigned count;

   public:
   /// Constructor
   AvgTime();

   /// Add an interval
   void add(const Timestamp& start,const Timestamp& end);
   /// The avg time in ms
   double avg() const;
};
//---------------------------------------------------------------------------
#endif
