#include <fstream>
#include <iostream>
#include <vector>
#include <algorithm>
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
namespace {
//---------------------------------------------------------------------------
/// A RDF tripple
struct Tripple {
   /// The values as IDs
   unsigned subject,predicate,object;
};
//---------------------------------------------------------------------------
/// Order a RDF tripple lexicographically
struct OrderTripple {
   bool operator()(const Tripple& a,const Tripple& b) const {
      return (a.subject<b.subject)||
             ((a.subject==b.subject)&&((a.predicate<b.predicate)||
             ((a.predicate==b.predicate)&&(a.object<b.object))));
   }
};
//---------------------------------------------------------------------------
bool readFacts(vector<Tripple>& facts,const char* fileName)
   // Read the facts table
{
   ifstream in(fileName);
   if (!in.is_open()) {
      cout << "unable to open " << fileName << endl;
      return false;
   }

   facts.clear();
   while (true) {
      Tripple t;
      in >> t.subject >> t.predicate >> t.object;
      if (!in.good()) break;
      facts.push_back(t);
   }

   return true;
}
//---------------------------------------------------------------------------
void storeUnsigned32(unsigned char* target,unsigned value)
   // Store an unsigned 32-bit value
{
   target[0]=(value>>24)&0xFF;
   target[1]=(value>>16)&0xFF;
   target[2]=(value>>8)&0xFF;
   target[3]=value&0xFF;
}
//---------------------------------------------------------------------------
unsigned bytes(unsigned v) { if (v>=(1<<24)) return 4; else if (v>=(1<<16)) return 3; else if (v>=(1<<8)) return 2; else return 1; }
//---------------------------------------------------------------------------
void pack(const vector<Tripple>& facts)
   // Pack the facts using prefix compression
{
   unsigned size=0;
   unsigned lastS=0,lastP=0,lastO=0;

   unsigned skipped=0;
   for (vector<Tripple>::const_iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter) {
      // Access the current tripple
      unsigned subject=(*iter).subject,predicate=(*iter).predicate,object=(*iter).object;

      // Compute the compressed length
      unsigned len;
      if (subject==lastS) {
         if (predicate==lastP) {
            if (object==lastO) {
               if (!skipped)
                  cout << "warning: duplicate item skipped" << endl;
               skipped++;
               continue;
            } else {
               if ((object-lastO)<128)
                  len=1; else
                  len=1+bytes(object-lastO-128);
            }
         } else {
            len=1+bytes(predicate-lastP)+bytes(object);
         }
      } else {
         len=1+bytes(subject-lastS)+bytes(predicate)+bytes(object);
      }
      lastS=subject; lastP=predicate; lastO=object;
      size+=len;
   }
   if (skipped)
      cout << "skipped " << skipped << " items out of " << facts.size() << endl;
   cout << "uncompressed size: " << ((facts.size()-skipped)*12) << endl;
   cout << "compressed size:   " << size << " bytes" << endl;
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   if (argc!=2) {
      cout << "usage: " << argv[0] << " <facts>" << endl;
      return 1;
   }

   // Read the facts table
   vector<Tripple> facts;
   if (!readFacts(facts,argv[1]))
      return 1;

   // Sort it
   sort(facts.begin(),facts.end(),OrderTripple());

   // And compute the compressed size
   pack(facts);
}
//---------------------------------------------------------------------------
