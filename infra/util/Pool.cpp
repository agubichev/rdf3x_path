#include "infra/util/Pool.hpp"
//---------------------------------------------------------------------------
void* PoolHelper::sort(void* data)
   // Sort a single-linked list using merge sort
{
#define next(x) *reinterpret_cast<void**>(x)
   // Special cases first
   if (!data)
      return data;

   // Split
   void* left=data,*leftIter=data;
   if ((data=next(data))==0) return left;
   void *right=data,*rightIter=data;
   if ((data=next(data))==0) {
      if (left<right) return left;
      next(right)=left; next(left)=0;
      return right;
   }
   for (;data;data=next(data)) {
      leftIter=next(leftIter)=data;
      if ((data=next(data))==0) break;
      rightIter=(next(rightIter)=data);
   }
   next(leftIter)=0; next(rightIter)=0;

   // Sort recursive
   left=sort(left);
   right=sort(right);

   // And merge
   void* result;
   if (left<right) {
      result=left; left=next(left);
   } else {
      result=right; right=next(right);
   }
   void* iter;
   for (iter=result;left&&right;)
      if (left<right) {
         iter=(next(iter)=left); left=next(left);
      } else {
         iter=(next(iter)=right); right=next(right);
      }
   if (left) {
      next(iter)=left;
   } else if (right) {
      next(iter)=right;
   }
   return result;
#undef next
}
//---------------------------------------------------------------------------
