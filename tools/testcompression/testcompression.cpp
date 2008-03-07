#include "infra/util/Hash.hpp"
#include "infra/osdep/Timestamp.hpp"
#include "rts/database/Database.hpp"
#include <fstream>
#include <iostream>
#include <vector>
#include <algorithm>
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
void buildStatisticsPage(Database& db,Database::DataOrder order,unsigned char* page);
//---------------------------------------------------------------------------
namespace {
//---------------------------------------------------------------------------
/// A RDF tripple
struct Triple {
   /// The values as IDs
   unsigned subject,predicate,object;
};
//---------------------------------------------------------------------------
/// Order a RDF tripple lexicographically
struct OrderTriple {
   bool operator()(const Triple& a,const Triple& b) const {
      return (a.subject<b.subject)||
             ((a.subject==b.subject)&&((a.predicate<b.predicate)||
             ((a.predicate==b.predicate)&&(a.object<b.object))));
   }
};
//---------------------------------------------------------------------------
bool readFacts(vector<Triple>& facts,const char* fileName)
   // Read the facts table
{
   ifstream in(fileName);
   if (!in.is_open()) {
      cout << "unable to open " << fileName << endl;
      return false;
   }

   facts.clear();
   while (true) {
      Triple t;
      in >> t.subject >> t.predicate >> t.object;
      if (!in.good()) break;
      facts.push_back(t);
   }

   return true;
}
//---------------------------------------------------------------------------
// Dummy to force computations
unsigned force = 0;
//---------------------------------------------------------------------------
unsigned bytes(unsigned v)
   // Compute the number of bytes required to encode a value
{
   if (v>=(1<<24))
      return 4; else
   if (v>=(1<<16))
      return 3; else
   if (v>=(1<<8)) return 2; else
      return 1;
}
//---------------------------------------------------------------------------
unsigned writeVar(unsigned char* buffer,unsigned ofs,unsigned value)
   // Write an integer with varying size
{
   if (value>=(1<<24)) {
      buffer[ofs]=value>>24;
      buffer[ofs+1]=(value>>16)&0xFF;
      buffer[ofs+1]=(value>>8)&0xFF;
      buffer[ofs+2]=value&0xFF;
      return ofs+4;
   } else if (value>=(1<<16)) {
      buffer[ofs]=value>>16;
      buffer[ofs+1]=(value>>8)&0xFF;
      buffer[ofs+2]=value&0xFF;
      return ofs+3;
   } else if (value>=(1<<8)) {
      buffer[ofs]=value>>8;
      buffer[ofs+1]=value&0xFF;
      return ofs+2;
   } else {
      buffer[ofs]=value;
      return ofs+1;
   }
}
//---------------------------------------------------------------------------
static inline unsigned readDelta1(const unsigned char* pos) { return pos[0]; }
static unsigned readDelta2(const unsigned char* pos) { return (pos[0]<<8)|pos[1]; }
static unsigned readDelta3(const unsigned char* pos) { return (pos[0]<<16)|(pos[1]<<8)|pos[2]; }
static unsigned readDelta4(const unsigned char* pos) { return (pos[0]<<24)|(pos[1]<<16)|(pos[2]<<8)|pos[3]; }
//---------------------------------------------------------------------------
static void testRDFStore(const vector<Triple>& facts,unsigned* sizes,unsigned* times,unsigned char* buffer)
   // Compute the different statistics
{
   unsigned lastSubject=0,lastPredicate=0,lastObject=0;

   // Compress
   unsigned bufferPos=0;
   for (vector<Triple>::const_iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter) {
      // Access the current tripple
      unsigned subject=(*iter).subject,predicate=(*iter).predicate,object=(*iter).object;

      // And store it
      if (subject==lastSubject) {
         if (predicate==lastPredicate) {
            if (object==lastObject) {
               // Skipping a duplicate
               continue;
            } else {
               if ((object-lastObject)<128) {
                  buffer[bufferPos++]=object-lastObject;
               } else {
                  buffer[bufferPos++]=0x80|(bytes(object-lastObject-128)-1);
                  bufferPos=writeVar(buffer,bufferPos,object-lastObject-128);
               }
            }
         } else {
            buffer[bufferPos++]=0x80|(bytes(predicate-lastPredicate)<<2)|(bytes(object)-1);
            bufferPos=writeVar(buffer,bufferPos,predicate-lastPredicate);
            bufferPos=writeVar(buffer,bufferPos,object);
         }
      } else {
         buffer[bufferPos++]=0xC0|((bytes(subject-lastSubject)-1)<<4)|((bytes(predicate)-1)<<2)|(bytes(object)-1);
         bufferPos=writeVar(buffer,bufferPos,subject-lastSubject);
         bufferPos=writeVar(buffer,bufferPos,predicate);
         bufferPos=writeVar(buffer,bufferPos,object);
      }

      // Update the values
      lastSubject=subject; lastPredicate=predicate; lastObject=object;
   }
   cout << "RDFstore " << bufferPos << " bytes ";
   sizes[0]+=bufferPos;
   cout.flush();

   Timestamp start;
   // Decompress again
   unsigned value1=0,value2=0,value3=0;
   for (const unsigned char* reader=buffer,*limit=buffer+bufferPos;reader<limit;) {
      // Decode the header byte
      unsigned info=*(reader++);
      // Small gap only?
      if (info<0x80) {
         if (!info)
            break;
         value3+=info;
         continue;
      }
      // Decode it
      switch (info&127) {
         case 0: value3+=readDelta1(reader)+128; reader+=1; break;
         case 1: value3+=readDelta2(reader)+128; reader+=2; break;
         case 2: value3+=readDelta3(reader)+128; reader+=3; break;
         case 3: value3+=readDelta4(reader)+128; reader+=4; break;
         case 4: value2+=readDelta1(reader); value3=readDelta1(reader+1); reader+=2; break;
         case 5: value2+=readDelta1(reader); value3=readDelta2(reader+1); reader+=3; break;
         case 6: value2+=readDelta1(reader); value3=readDelta3(reader+1); reader+=4; break;
         case 7: value2+=readDelta1(reader); value3=readDelta4(reader+1); reader+=5; break;
         case 8: value2+=readDelta2(reader); value3=readDelta1(reader+2); reader+=3; break;
         case 9: value2+=readDelta2(reader); value3=readDelta2(reader+2); reader+=4; break;
         case 10: value2+=readDelta2(reader); value3=readDelta3(reader+2); reader+=5; break;
         case 11: value2+=readDelta2(reader); value3=readDelta4(reader+2); reader+=6; break;
         case 12: value2+=readDelta3(reader); value3=readDelta1(reader+3); reader+=4; break;
         case 13: value2+=readDelta3(reader); value3=readDelta2(reader+3); reader+=5; break;
         case 14: value2+=readDelta3(reader); value3=readDelta3(reader+3); reader+=6; break;
         case 15: value2+=readDelta3(reader); value3=readDelta4(reader+3); reader+=7; break;
         case 16: value2+=readDelta4(reader); value3=readDelta1(reader+4); reader+=5; break;
         case 17: value2+=readDelta4(reader); value3=readDelta2(reader+4); reader+=6; break;
         case 18: value2+=readDelta4(reader); value3=readDelta3(reader+4); reader+=7; break;
         case 19: value2+=readDelta4(reader); value3=readDelta4(reader+4); reader+=8; break;
         case 20: case 21: case 22: case 23: case 24: case 25: case 26: case 27: case 28: case 29: case 30: case 31: break;
         case 32: case 33: case 34: case 35: case 36: case 37: case 38: case 39: case 40: case 41: case 42: case 43:
         case 44: case 45: case 46: case 47: case 48: case 49: case 50: case 51: case 52: case 53: case 54: case 55:
         case 56: case 57: case 58: case 59: case 60: case 61: case 62: case 63:
         case 64: value1+=readDelta1(reader); value2=readDelta1(reader+1); value3=readDelta1(reader+2); reader+=3; break;
         case 65: value1+=readDelta1(reader); value2=readDelta1(reader+1); value3=readDelta2(reader+2); reader+=4; break;
         case 66: value1+=readDelta1(reader); value2=readDelta1(reader+1); value3=readDelta3(reader+2); reader+=5; break;
         case 67: value1+=readDelta1(reader); value2=readDelta1(reader+1); value3=readDelta4(reader+2); reader+=6; break;
         case 68: value1+=readDelta1(reader); value2=readDelta2(reader+1); value3=readDelta1(reader+3); reader+=4; break;
         case 69: value1+=readDelta1(reader); value2=readDelta2(reader+1); value3=readDelta2(reader+3); reader+=5; break;
         case 70: value1+=readDelta1(reader); value2=readDelta2(reader+1); value3=readDelta3(reader+3); reader+=6; break;
         case 71: value1+=readDelta1(reader); value2=readDelta2(reader+1); value3=readDelta4(reader+3); reader+=7; break;
         case 72: value1+=readDelta1(reader); value2=readDelta3(reader+1); value3=readDelta1(reader+4); reader+=5; break;
         case 73: value1+=readDelta1(reader); value2=readDelta3(reader+1); value3=readDelta2(reader+4); reader+=6; break;
         case 74: value1+=readDelta1(reader); value2=readDelta3(reader+1); value3=readDelta3(reader+4); reader+=7; break;
         case 75: value1+=readDelta1(reader); value2=readDelta3(reader+1); value3=readDelta4(reader+4); reader+=8; break;
         case 76: value1+=readDelta1(reader); value2=readDelta4(reader+1); value3=readDelta1(reader+5); reader+=6; break;
         case 77: value1+=readDelta1(reader); value2=readDelta4(reader+1); value3=readDelta2(reader+5); reader+=7; break;
         case 78: value1+=readDelta1(reader); value2=readDelta4(reader+1); value3=readDelta3(reader+5); reader+=8; break;
         case 79: value1+=readDelta1(reader); value2=readDelta4(reader+1); value3=readDelta4(reader+5); reader+=9; break;
         case 80: value1+=readDelta2(reader); value2=readDelta1(reader+2); value3=readDelta1(reader+3); reader+=4; break;
         case 81: value1+=readDelta2(reader); value2=readDelta1(reader+2); value3=readDelta2(reader+3); reader+=5; break;
         case 82: value1+=readDelta2(reader); value2=readDelta1(reader+2); value3=readDelta3(reader+3); reader+=6; break;
         case 83: value1+=readDelta2(reader); value2=readDelta1(reader+2); value3=readDelta4(reader+3); reader+=7; break;
         case 84: value1+=readDelta2(reader); value2=readDelta2(reader+2); value3=readDelta1(reader+4); reader+=5; break;
         case 85: value1+=readDelta2(reader); value2=readDelta2(reader+2); value3=readDelta2(reader+4); reader+=6; break;
         case 86: value1+=readDelta2(reader); value2=readDelta2(reader+2); value3=readDelta3(reader+4); reader+=7; break;
         case 87: value1+=readDelta2(reader); value2=readDelta2(reader+2); value3=readDelta4(reader+4); reader+=8; break;
         case 88: value1+=readDelta2(reader); value2=readDelta3(reader+2); value3=readDelta1(reader+5); reader+=6; break;
         case 89: value1+=readDelta2(reader); value2=readDelta3(reader+2); value3=readDelta2(reader+5); reader+=7; break;
         case 90: value1+=readDelta2(reader); value2=readDelta3(reader+2); value3=readDelta3(reader+5); reader+=8; break;
         case 91: value1+=readDelta2(reader); value2=readDelta3(reader+2); value3=readDelta4(reader+5); reader+=9; break;
         case 92: value1+=readDelta2(reader); value2=readDelta4(reader+2); value3=readDelta1(reader+6); reader+=7; break;
         case 93: value1+=readDelta2(reader); value2=readDelta4(reader+2); value3=readDelta2(reader+6); reader+=8; break;
         case 94: value1+=readDelta2(reader); value2=readDelta4(reader+2); value3=readDelta3(reader+6); reader+=9; break;
         case 95: value1+=readDelta2(reader); value2=readDelta4(reader+2); value3=readDelta4(reader+6); reader+=10; break;
         case 96: value1+=readDelta3(reader); value2=readDelta1(reader+3); value3=readDelta1(reader+4); reader+=5; break;
         case 97: value1+=readDelta3(reader); value2=readDelta1(reader+3); value3=readDelta2(reader+4); reader+=6; break;
         case 98: value1+=readDelta3(reader); value2=readDelta1(reader+3); value3=readDelta3(reader+4); reader+=7; break;
         case 99: value1+=readDelta3(reader); value2=readDelta1(reader+3); value3=readDelta4(reader+4); reader+=8; break;
         case 100: value1+=readDelta3(reader); value2=readDelta2(reader+3); value3=readDelta1(reader+5); reader+=6; break;
         case 101: value1+=readDelta3(reader); value2=readDelta2(reader+3); value3=readDelta2(reader+5); reader+=7; break;
         case 102: value1+=readDelta3(reader); value2=readDelta2(reader+3); value3=readDelta3(reader+5); reader+=8; break;
         case 103: value1+=readDelta3(reader); value2=readDelta2(reader+3); value3=readDelta4(reader+5); reader+=9; break;
         case 104: value1+=readDelta3(reader); value2=readDelta3(reader+3); value3=readDelta1(reader+6); reader+=7; break;
         case 105: value1+=readDelta3(reader); value2=readDelta3(reader+3); value3=readDelta2(reader+6); reader+=8; break;
         case 106: value1+=readDelta3(reader); value2=readDelta3(reader+3); value3=readDelta3(reader+6); reader+=9; break;
         case 107: value1+=readDelta3(reader); value2=readDelta3(reader+3); value3=readDelta4(reader+6); reader+=10; break;
         case 108: value1+=readDelta3(reader); value2=readDelta4(reader+3); value3=readDelta1(reader+7); reader+=8; break;
         case 109: value1+=readDelta3(reader); value2=readDelta4(reader+3); value3=readDelta2(reader+7); reader+=9; break;
         case 110: value1+=readDelta3(reader); value2=readDelta4(reader+3); value3=readDelta3(reader+7); reader+=10; break;
         case 111: value1+=readDelta3(reader); value2=readDelta4(reader+3); value3=readDelta4(reader+7); reader+=11; break;
         case 112: value1+=readDelta4(reader); value2=readDelta1(reader+4); value3=readDelta1(reader+5); reader+=6; break;
         case 113: value1+=readDelta4(reader); value2=readDelta1(reader+4); value3=readDelta2(reader+5); reader+=7; break;
         case 114: value1+=readDelta4(reader); value2=readDelta1(reader+4); value3=readDelta3(reader+5); reader+=8; break;
         case 115: value1+=readDelta4(reader); value2=readDelta1(reader+4); value3=readDelta4(reader+5); reader+=9; break;
         case 116: value1+=readDelta4(reader); value2=readDelta2(reader+4); value3=readDelta1(reader+6); reader+=7; break;
         case 117: value1+=readDelta4(reader); value2=readDelta2(reader+4); value3=readDelta2(reader+6); reader+=8; break;
         case 118: value1+=readDelta4(reader); value2=readDelta2(reader+4); value3=readDelta3(reader+6); reader+=9; break;
         case 119: value1+=readDelta4(reader); value2=readDelta2(reader+4); value3=readDelta4(reader+6); reader+=10; break;
         case 120: value1+=readDelta4(reader); value2=readDelta3(reader+4); value3=readDelta1(reader+7); reader+=8; break;
         case 121: value1+=readDelta4(reader); value2=readDelta3(reader+4); value3=readDelta2(reader+7); reader+=9; break;
         case 122: value1+=readDelta4(reader); value2=readDelta3(reader+4); value3=readDelta3(reader+7); reader+=10; break;
         case 123: value1+=readDelta4(reader); value2=readDelta3(reader+4); value3=readDelta4(reader+7); reader+=11; break;
         case 124: value1+=readDelta4(reader); value2=readDelta4(reader+4); value3=readDelta1(reader+8); reader+=9; break;
         case 125: value1+=readDelta4(reader); value2=readDelta4(reader+4); value3=readDelta2(reader+8); reader+=10; break;
         case 126: value1+=readDelta4(reader); value2=readDelta4(reader+4); value3=readDelta3(reader+8); reader+=11; break;
         case 127: value1+=readDelta4(reader); value2=readDelta4(reader+4); value3=readDelta4(reader+8); reader+=12; break;
      }
   }
   Timestamp stop;

   force+=value1+value2+value3;

   cout << (stop-start) << " ms" << endl;
   times[0]+=stop-start;
}
//---------------------------------------------------------------------------
static inline bool readBit(const unsigned char*& reader,unsigned& ofs)
   // Read a bit
{
   if (ofs>=8) {
      ofs=1;
      return (*(++reader))&1;
   } else {
      return (*reader)&(1<<(ofs++));
   }
}
//---------------------------------------------------------------------------
static inline void writeBit(unsigned char*& writer,unsigned& ofs,bool b)
   // Write a bit
{
   if (ofs>=8) {
      ofs=1;
      ++writer;
      *writer=static_cast<unsigned char>(((*writer)&0xFE)|b);
   } else {
      *writer=static_cast<unsigned char>(((*writer)&(~(1<<ofs)))|(b<<ofs));
      ++ofs;
   }
}
//---------------------------------------------------------------------------
static unsigned readGamma(const unsigned char*& reader,unsigned& ofs)
{
   unsigned len=0;
   while (readBit(reader,ofs))
      ++len;
   unsigned result=0;
   for (;len;--len)
      result=(result<<1)|readBit(reader,ofs);
   return result;
}
//---------------------------------------------------------------------------
static void writeGamma(unsigned char*& writer,unsigned& ofs,unsigned k)
   // Write a gamma value
{
   unsigned kc=k,len=0;
   while (kc) {
      writeBit(writer,ofs,1);
      kc>>=1;
      len++;
   }
   writeBit(writer,ofs,0);
   for (unsigned index=0;index<len;index++)
      writeBit(writer,ofs,k&(1<<(len-index-1)));
}
//---------------------------------------------------------------------------
static void testGamma(const vector<Triple>& facts,unsigned* sizes,unsigned* times,unsigned char* buffer)
   // Compute the different statistics
{
   unsigned lastSubject=0,lastPredicate=0,lastObject=0;

   unsigned char* writer=buffer;
   unsigned ofs=0;


   // Compress
   for (vector<Triple>::const_iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter) {
      // Access the current tripple
      unsigned subject=(*iter).subject,predicate=(*iter).predicate,object=(*iter).object;

      // Compute the other statistics
      {
         if (subject==lastSubject) {
            if (predicate==lastPredicate) {
               if (object==lastObject) {
                  // Skipping a duplicate
                  continue;
               } else {
                  writeBit(writer,ofs,0);
                  writeBit(writer,ofs,0);
                  writeGamma(writer,ofs,object-lastObject);
               }
            } else {
               writeBit(writer,ofs,0);
               writeBit(writer,ofs,1);
               writeGamma(writer,ofs,predicate-lastPredicate);
               writeGamma(writer,ofs,object);
            }
         } else {
            writeBit(writer,ofs,1);
            writeBit(writer,ofs,0);
            writeGamma(writer,ofs,subject-lastSubject);
            writeGamma(writer,ofs,predicate);
            writeGamma(writer,ofs,object);
         }
      }

      // Update the values
      lastSubject=subject; lastPredicate=predicate; lastObject=object;
   }
   writeBit(writer,ofs,1);
   writeBit(writer,ofs,1);
   cout << "Gamma    " << (writer-buffer) << " bytes ";
   cout.flush();
   sizes[1]+=writer-buffer;

   // Decompression
   Timestamp start;
   const unsigned char* reader=buffer;
   ofs=0;
   unsigned value1=0,value2=0,value3=0;
   while (true) {
      if (readBit(reader,ofs)) {
         if (readBit(reader,ofs)) {
            break;
         } else {
            value1+=readGamma(reader,ofs);
            value2=readGamma(reader,ofs);
            value3=readGamma(reader,ofs);
         }
      } else {
         if (readBit(reader,ofs)) {
            value2+=readGamma(reader,ofs);
            value3=readGamma(reader,ofs);
         } else {
            value3+=readGamma(reader,ofs);
         }
      }
   }
   Timestamp stop;

   force+=value1+value2+value3;

   cout << (stop-start) << " ms" << endl;
   times[1]+=stop-start;
}
//---------------------------------------------------------------------------
static unsigned readDelta(const unsigned char*& reader,unsigned& ofs)
{
   unsigned len=readGamma(reader,ofs);
   unsigned result=0;
   for (;len;--len)
      result=(result<<1)|readBit(reader,ofs);
   return result;
}
//---------------------------------------------------------------------------
static void writeDelta(unsigned char*& writer,unsigned& ofs,unsigned k)
   // Write a gamma value
{
   unsigned len=0;
   for (unsigned kc=k;kc;) {
      ++len;
      kc>>=1;
   }
   writeGamma(writer,ofs,len);
   for (unsigned index=0;index<len;index++)
      writeBit(writer,ofs,k&(1<<(len-index-1)));
}
//---------------------------------------------------------------------------
static void testDelta(const vector<Triple>& facts,unsigned* sizes,unsigned* times,unsigned char* buffer)
   // Compute the different statistics
{
   unsigned lastSubject=0,lastPredicate=0,lastObject=0;

   unsigned char* writer=buffer;
   unsigned ofs=0;


   // Compress
   for (vector<Triple>::const_iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter) {
      // Access the current tripple
      unsigned subject=(*iter).subject,predicate=(*iter).predicate,object=(*iter).object;

      // Compute the other statistics
      {
         if (subject==lastSubject) {
            if (predicate==lastPredicate) {
               if (object==lastObject) {
                  // Skipping a duplicate
                  continue;
               } else {
                  writeBit(writer,ofs,0);
                  writeBit(writer,ofs,0);
                  writeDelta(writer,ofs,object-lastObject);
               }
            } else {
               writeBit(writer,ofs,0);
               writeBit(writer,ofs,1);
               writeDelta(writer,ofs,predicate-lastPredicate);
               writeDelta(writer,ofs,object);
            }
         } else {
            writeBit(writer,ofs,1);
            writeBit(writer,ofs,0);
            writeDelta(writer,ofs,subject-lastSubject);
            writeDelta(writer,ofs,predicate);
            writeDelta(writer,ofs,object);
         }
      }

      // Update the values
      lastSubject=subject; lastPredicate=predicate; lastObject=object;
   }
   writeBit(writer,ofs,1);
   writeBit(writer,ofs,1);
   cout << "Delta    " << (writer-buffer) << " bytes ";
   cout.flush();
   sizes[2]+=writer-buffer;

   // Decompression
   Timestamp start;
   const unsigned char* reader=buffer;
   ofs=0;
   unsigned value1=0,value2=0,value3=0;
   while (true) {
      if (readBit(reader,ofs)) {
         if (readBit(reader,ofs)) {
            break;
         } else {
            value1+=readDelta(reader,ofs);
            value2=readDelta(reader,ofs);
            value3=readDelta(reader,ofs);
         }
      } else {
         if (readBit(reader,ofs)) {
            value2+=readDelta(reader,ofs);
            value3=readDelta(reader,ofs);
         } else {
            value3+=readDelta(reader,ofs);
         }
      }
   }
   Timestamp stop;

   force+=value1+value2+value3;

   cout << (stop-start) << " ms" << endl;
   times[2]+=stop-start;
}
//---------------------------------------------------------------------------
static unsigned readGolomb(const unsigned char*& reader,unsigned& ofs,unsigned b)
{
   /// XXX reads the junk from writeGolomb
   unsigned l=0;
   unsigned r=0;
   while (readBit(reader,ofs)) {
      ++l;
      r=(1<<r)|1;
   }

   return l*b+r;
}
//---------------------------------------------------------------------------
static void writeGolomb(unsigned char*& writer,unsigned& ofs,unsigned b,unsigned k)
{
   // XXX writes junk
   unsigned q=static_cast<unsigned>((k-1)/b);
   unsigned r=k-(q*b)-1;

   unsigned l=q+1;
   while (r) {
      r>>=1;
      l++;
   }

   for (unsigned index=0;index<l;index++)
      writeBit(writer,ofs,1);
   writeBit(writer,ofs,0);
}
//---------------------------------------------------------------------------
static void testGolomb(const vector<Triple>& facts,unsigned* sizes,unsigned* times,unsigned char* buffer)
   // Compute the different statistics
{
   unsigned lastSubject=0,lastPredicate=0,lastObject=0;

   // Derive the golomb parameters
   unsigned g1,g2,g3,g2f,g3f;
   {
      unsigned long long g1v=0,g1c=0,g2v=0,g2c=0,g2fv=0,g2fc=0,g3v=0,g3c=0,g3fv=0,g3fc=0;
      for (vector<Triple>::const_iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter) {
         // Access the current tripple
         unsigned subject=(*iter).subject,predicate=(*iter).predicate,object=(*iter).object;

         if (subject==lastSubject) {
            if (predicate==lastPredicate) {
               if (object==lastObject) {
                  // Skipping a duplicate
                  continue;
               } else {
                  g3v+=object-lastObject;
                  g3c++;
               }
            } else {
               g2v+=predicate-lastPredicate;
               g2c++;
               g3fv+=object;
               g3fc++;
            }
         } else {
            g1v+=subject-lastSubject;
            g1c++;
            g2fv+=predicate;
            g2fc++;
            g3fv+=object;
            g3fc++;
         }
         // Update the values
         lastSubject=subject; lastPredicate=predicate; lastObject=object;
      }
      lastSubject=0; lastPredicate=0; lastObject=0;

      if (!g1c) g1c=1;
      g1=static_cast<unsigned>(0.69*g1v/g1c)+1;
      if (!g2c) g2c=1;
      g2=static_cast<unsigned>(0.69*g2v/g2c)+1;
      if (!g3c) g3c=1;
      g3=static_cast<unsigned>(0.69*g3v/g3c)+1;
      if (!g2fc) g2fc=1;
      g2f=static_cast<unsigned>(0.69*g2fv/g2fc)+1;
      if (!g3fc) g3fc=1;
      g3f=static_cast<unsigned>(0.69*g3fv/g3fc)+1;
   }


   unsigned char* writer=buffer;
   unsigned ofs=0;

   // Compress
   for (vector<Triple>::const_iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter) {
      // Access the current tripple
      unsigned subject=(*iter).subject,predicate=(*iter).predicate,object=(*iter).object;

      // Compute the other statistics
      {
         if (subject==lastSubject) {
            if (predicate==lastPredicate) {
               if (object==lastObject) {
                  // Skipping a duplicate
                  continue;
               } else {
                  writeBit(writer,ofs,0);
                  writeBit(writer,ofs,0);
                  writeGolomb(writer,ofs,g3,object-lastObject);
               }
            } else {
               writeBit(writer,ofs,0);
               writeBit(writer,ofs,1);
               writeGolomb(writer,ofs,g2,predicate-lastPredicate);
               writeGolomb(writer,ofs,g3f,object);
            }
         } else {
            writeBit(writer,ofs,1);
            writeBit(writer,ofs,0);
            writeGolomb(writer,ofs,g1,subject-lastSubject);
            writeGolomb(writer,ofs,g2f,predicate);
            writeGolomb(writer,ofs,g3f,object);
         }
      }

      // Update the values
      lastSubject=subject; lastPredicate=predicate; lastObject=object;
   }
   writeBit(writer,ofs,1);
   writeBit(writer,ofs,1);
   cout << "Golomb   " << (writer-buffer) << " bytes ";
   cout.flush();
   sizes[3]+=writer-buffer;

   // Decompression
   Timestamp start;
   const unsigned char* reader=buffer;
   ofs=0;
   unsigned value1=0,value2=0,value3=0;
   while (true) {
      if (readBit(reader,ofs)) {
         if (readBit(reader,ofs)) {
            break;
         } else {
            value1+=readGolomb(reader,ofs,g1);
            value2=readGolomb(reader,ofs,g2f);
            value3=readGolomb(reader,ofs,g3f);
         }
      } else {
         if (readBit(reader,ofs)) {
            value2+=readGolomb(reader,ofs,g2);
            value3=readGolomb(reader,ofs,g3f);
         } else {
            value3+=readGolomb(reader,ofs,g3);
         }
      }
   }
   Timestamp stop;

   force+=value1+value2+value3;

   cout << (stop-start) << " ms" << endl;
   times[3]+=stop-start;
}
//---------------------------------------------------------------------------
void processFacts(vector<Triple>& facts)
   // Dump all 6 orderings into the database
{
   unsigned sizes[4]={0,0,0,0};
   unsigned times[4]={0,0,0,0};
   unsigned char* buffer=new unsigned char[facts.size()*20];


   // Produce the different orderings
   cout << "Full size: " << facts.size()*12 << " bytes" << endl;
   for (unsigned index=0;index<6;index++) {
      cout << "Dumping ordering " << (index+1) << endl;

      // Change the values to fit the desired order
      switch (index) {
         case 0: // subject,predicate,object
            break;
         case 1: // subject,object,predicate
            for (vector<Triple>::iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter)
               std::swap((*iter).object,(*iter).predicate);
            break;
         case 2: // object,predicate,subject
            for (vector<Triple>::iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter)
               std::swap((*iter).object,(*iter).subject);
            break;
         case 3: // object,subject,predicate
            for (vector<Triple>::iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter) {
               std::swap((*iter).object,(*iter).subject);
               std::swap((*iter).object,(*iter).predicate);
            }
            break;
         case 4: // predicate,subject,object
            for (vector<Triple>::iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter)
               std::swap((*iter).subject,(*iter).predicate);
            break;
         case 5: // predicate,object,subject
            for (vector<Triple>::iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter) {
               std::swap((*iter).subject,(*iter).predicate);
               std::swap((*iter).object,(*iter).predicate);
            }
            break;
      }

      // Sort the facts accordingly
      sort(facts.begin(),facts.end(),OrderTriple());

      // Compute the statistics
      testRDFStore(facts,sizes,times,buffer);
      testGamma(facts,sizes,times,buffer);
      testDelta(facts,sizes,times,buffer);
      testGolomb(facts,sizes,times,buffer);

      // Change the values back to the original order
      switch (index) {
         case 0: // subject,predicate,object
            break;
         case 1: // subject,object,predicate
            for (vector<Triple>::iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter)
               std::swap((*iter).object,(*iter).predicate);
            break;
         case 2: // object,predicate,subject
            for (vector<Triple>::iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter)
               std::swap((*iter).object,(*iter).subject);
            break;
         case 3: // object,subject,predicate
            for (vector<Triple>::iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter) {
               std::swap((*iter).object,(*iter).predicate);
               std::swap((*iter).object,(*iter).subject);
            }
            break;
         case 4: // predicate,subject,object
            for (vector<Triple>::iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter)
               std::swap((*iter).subject,(*iter).predicate);
            break;
         case 5: // predicate,object,subject
            for (vector<Triple>::iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter) {
               std::swap((*iter).object,(*iter).predicate);
               std::swap((*iter).subject,(*iter).predicate);
            }
            break;
      }
   }
   delete[] buffer;

   cout << "Total:" << endl
        << "RDFstore " << sizes[0] << " bytes " << times[0] << " ms" << endl
        << "Gamma    " << sizes[1] << " bytes " << times[1] << " ms" << endl
        << "Delta    " << sizes[2] << " bytes " << times[2] << " ms" << endl
        << "Golomb   " << sizes[3] << " bytes " << times[3] << " ms" << endl;
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

   { unsigned char buffer[100000];
   {
      unsigned char* writer=buffer;
      unsigned ofs=0;
      buffer[0]=0; buffer[1]=0; buffer[2]=0; buffer[3]=0; buffer[4]=0;
      for (unsigned index=0;index<8;index++) {
         writeBit(writer,ofs,0);
         cout << (unsigned)buffer[0] << ' ';
      }
      for (unsigned index=0;index<8;index++) {
         writeBit(writer,ofs,1);
         cout << (unsigned)buffer[1] << ' ';
      }
      for (unsigned index=0;index<8;index++) {
         writeBit(writer,ofs,index&1);
         cout << (unsigned)buffer[2] << ' ';
      }
      for (unsigned index=0;index<8;index++) {
         writeBit(writer,ofs,!(index&1));
         cout << (unsigned)buffer[3] << ' ';
      }
      cout << endl;
   }
   {
      const unsigned char* reader=buffer;
      unsigned ofs=0;
      for (unsigned index=0;index<8*4;index++)
         cout << readBit(reader,ofs);
      cout << endl;
   }
   {
      unsigned char* writer=buffer;
      unsigned ofs=0;
      for (unsigned index=0;index<32;index++)
         writeDelta(writer,ofs,index);
   }
   {
      const unsigned char* reader=buffer;
      unsigned ofs=0;
      for (unsigned index=0;index<32;index++)
         cout << readBit(reader,ofs) << " ";
      cout << endl;
   }
   {
      const unsigned char* reader=buffer;
      unsigned ofs=0;
      for (unsigned index=0;index<32;index++) {
         unsigned v=readDelta(reader,ofs);
         if (v!=index)
            cout << "expected " << index << ", got " << v << endl;
      }
   }
   }

   // Read the facts table
   cout << "Reading the facts table..." << endl;
   vector<Triple> facts;
   if (!readFacts(facts,argv[1]))
      return 1;

   // Eliminating duplicates
   cout << "Eliminating duplicates..." << endl;
   {
      sort(facts.begin(),facts.end(),OrderTriple());
      vector<Triple>::iterator writer=facts.begin();
      unsigned lastSubject=~0u,lastPredicate=~0u,lastObject=~0u;
      for (vector<Triple>::iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter)
         if ((((*iter).subject)!=lastSubject)||(((*iter).predicate)!=lastPredicate)||(((*iter).object)!=lastObject)) {
            *writer=*iter;
            ++writer;
            lastSubject=(*iter).subject; lastPredicate=(*iter).predicate; lastObject=(*iter).object;
         }
      facts.resize(writer-facts.begin());
   }

   // Produce the different orderings
   processFacts(facts);
}
//---------------------------------------------------------------------------
