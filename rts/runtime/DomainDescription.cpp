#include "rts/runtime/DomainDescription.hpp"
#include <cstring>
//---------------------------------------------------------------------------
// RDF-3X
// (c) 2008 Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
DomainDescription::DomainDescription(const DomainDescription& other)
   : min(other.min),max(other.max)
   // Copy-Constructor
{
   memcpy(filter,other.filter,sizeof(filter));
}
//---------------------------------------------------------------------------
DomainDescription& DomainDescription::operator=(const DomainDescription& other)
   // Assignment
{
   if (this!=&other) {
      min=other.min;
      max=other.max;
      memcpy(filter,other.filter,sizeof(filter));
   }
   return *this;
}
//---------------------------------------------------------------------------
bool DomainDescription::couldQualify(unsigned value) const
   // Could this value qualify?
{
   if ((value<min)||(value>max))
      return false;

   unsigned bit=value%(filterSize*filterEntryBits);
   return filter[bit/filterEntryBits]&(filterEntry1<<(bit%filterEntryBits));
}
//---------------------------------------------------------------------------
unsigned DomainDescription::nextCandidate(unsigned value) const
   // Return the next value >= value that could qualify (or ~0u)
{
   if (value<min) value=min;
   if (value>max) return ~0u;

   unsigned bit=value%(filterSize*filterEntryBits);
   if (filter[bit/filterEntryBits]&(filterEntry1<<(bit%filterEntryBits)))
      return value;

   if (~value)
      return value+1; else
      return value;
}
//---------------------------------------------------------------------------
PotentialDomainDescription::PotentialDomainDescription()
   // Constructor
{
   min=0;
   max=~0u;
   memset(filter,0xFF,sizeof(filter));
}
//---------------------------------------------------------------------------
void PotentialDomainDescription::sync(PotentialDomainDescription& other)
   // Synchronize with another domain, computing the intersection. Both are modified!
{
   if (min<other.min)
      min=other.min;
   if (min>other.min)
      other.min=min;
   if (max>other.max)
      max=other.max;
   if (max<other.max)
      other.max=max;
   for (unsigned index=0;index<filterSize;index++) {
      FilterEntry n=filter[index]&other.filter[index];
      filter[index]=n;
      other.filter[index]=n;
   }
}
//---------------------------------------------------------------------------
void PotentialDomainDescription::restrictTo(const ObservedDomainDescription& other)
   // Restrict to an observed domain
{
   if (min<other.min)
      min=other.min;
   if (max>other.max)
      max=other.max;
   for (unsigned index=0;index<filterSize;index++)
      filter[index]&=other.filter[index];
}
//---------------------------------------------------------------------------
ObservedDomainDescription::ObservedDomainDescription()
   // Constructor
{
   min=~0u;
   max=0;
   memset(filter,0,sizeof(filter));
}
//---------------------------------------------------------------------------
void ObservedDomainDescription::add(unsigned value)
   // Add an observed value
{
   if (value<min)
      min=value;
   if (value>max)
      max=value;

   unsigned bit=value%(filterSize*filterEntryBits);
   filter[bit/filterEntryBits]|=filterEntry1<<(bit%filterEntryBits);
}
//---------------------------------------------------------------------------
