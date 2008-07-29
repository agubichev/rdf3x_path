#include "infra/osdep/Timestamp.hpp"
#if defined(WIN32)||defined(__WIN32__)||defined(_WIN32)
#define CONFIG_WINDOWS
#endif
#ifdef CONFIG_WINDOWS
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#else
#include <sys/time.h>
#endif
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
Timestamp::Timestamp()
   // Constructor
{
#ifdef CONFIG_WINDOWS
   QueryPerformanceCounter(reinterpret_cast<LARGE_INTEGER*>(data));
#else
   gettimeofday(reinterpret_cast<timeval*>(data),0);
#endif
}
//---------------------------------------------------------------------------
unsigned Timestamp::operator-(const Timestamp& other) const
   // Difference in ms
{
#ifdef CONFIG_WINDOWS
   LARGE_INTEGER freq;
   QueryPerformanceFrequency(&freq);
   return static_cast<unsigned>(((reinterpret_cast<const LARGE_INTEGER*>(data)[0].QuadPart-reinterpret_cast<const LARGE_INTEGER*>(other.data)[0].QuadPart)*1000)/freq.QuadPart);
#else
   long long a=static_cast<long long>(reinterpret_cast<const timeval*>(data)->tv_sec)*1000+reinterpret_cast<const timeval*>(data)->tv_usec/1000;
   long long b=static_cast<long long>(reinterpret_cast<const timeval*>(other.data)->tv_sec)*1000+reinterpret_cast<const timeval*>(other.data)->tv_usec/1000;
   return a-b;
#endif
}
//---------------------------------------------------------------------------
AvgTime::AvgTime()
   : count(0)
   // Constructor
{
#ifdef CONFIG_WINDOWS
   *reinterpret_cast<__int64*>(data)=0;
#else
   *reinterpret_cast<long long*>(data)=0;
#endif
}
//---------------------------------------------------------------------------
void AvgTime::add(const Timestamp& start,const Timestamp& stop)
   // Add an interval
{
#ifdef CONFIG_WINDOWS
   *reinterpret_cast<__int64*>(data)+=reinterpret_cast<const LARGE_INTEGER*>(stop.data)[0].QuadPart-reinterpret_cast<const LARGE_INTEGER*>(start.data)[0].QuadPart;
#else
   long long a=static_cast<long long>(reinterpret_cast<const timeval*>(stop.data)->tv_sec)*1000000+reinterpret_cast<const timeval*>(stop.data)->tv_usec;
   long long b=static_cast<long long>(reinterpret_cast<const timeval*>(start.data)->tv_sec)*1000000+reinterpret_cast<const timeval*>(start.data)->tv_usec;
   *reinterpret_cast<long long*>(data)+=a-b;
#endif
   count++;
}
//---------------------------------------------------------------------------
double AvgTime::avg() const
   // Average
{
   if (!count) return 0;
   double val;
#ifdef CONFIG_WINDOWS
   LARGE_INTEGER freq;
   QueryPerformanceFrequency(&freq);
   val=(reinterpret_cast<const __int64*>(data)[0]*1000)/freq.QuadPart;
#else
   val=(*reinterpret_cast<const long long*>(data)/1000);
#endif
   val/=count;
   return val;
}
//---------------------------------------------------------------------------
