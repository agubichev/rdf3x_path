#include "TempFile.hpp"
#include <sstream>
#include <cassert>
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
/// The next id
unsigned TempFile::id = 0;
//---------------------------------------------------------------------------
string TempFile::newSuffix()
   // Construct a new suffix
{
   stringstream buffer;
   buffer << '.' << (id++);
   return buffer.str();
}
//---------------------------------------------------------------------------
TempFile::TempFile(const string& baseName)
   : baseName(baseName),fileName(baseName+newSuffix()),out(fileName.c_str()),writePointer(0)
   // Constructor
{
}
//---------------------------------------------------------------------------
TempFile::~TempFile()
   // Destructor
{
   discard();
}
//---------------------------------------------------------------------------
void TempFile::flush()
   // Flush the file
{
   if (writePointer) {
      out.write(writeBuffer,writePointer);
      writePointer=0;
   }
   out.flush();
}
//---------------------------------------------------------------------------
void TempFile::close()
   // Close the file
{
   flush();
   out.close();
}
//---------------------------------------------------------------------------
void TempFile::discard()
   // Discard the file
{
   close();
   remove(fileName.c_str());
}
//---------------------------------------------------------------------------
void TempFile::writeString(unsigned len,const char* str)
   // Write a string
{
   writeId(len);
   write(len,str);
}
//---------------------------------------------------------------------------
void TempFile::writeId(uint64_t id)
   // Write a id
{
   while (id>=128) {
      unsigned char c=static_cast<unsigned char>(id|128);
      if (writePointer==bufferSize) {
         out.write(writeBuffer,writePointer);
         writePointer=0;
      }
      writeBuffer[writePointer++]=c;
      id>>=7;
   }
   if (writePointer==bufferSize) {
      out.write(writeBuffer,writePointer);
      writePointer=0;
   }
   writeBuffer[writePointer++]=static_cast<unsigned char>(id);
}
//---------------------------------------------------------------------------
void TempFile::write(unsigned len,const char* data)
   // Raw write
{
   // Fill the buffer
   if (writePointer+len>bufferSize) {
      unsigned remaining=bufferSize-writePointer;
      memcpy(writeBuffer+writePointer,data,remaining);
      out.write(writeBuffer,bufferSize);
      writePointer=0;
      len-=remaining;
      data+=remaining;
   }
   // Write big chunks if any
   if (writePointer+len>bufferSize) {
      assert(writePointer==0);
      unsigned chunks=len/bufferSize;
      out.write(data,chunks*bufferSize);
      len-=chunks*bufferSize;
      data+=chunks*bufferSize;
   }
   // And fill the rest
   memcpy(writeBuffer+writePointer,data,len);
   writePointer+=len;
}
//---------------------------------------------------------------------------
const char* TempFile::skipId(const char* reader)
   // Skip an id
{
   while ((*reinterpret_cast<const unsigned char*>(reader))&128)
      ++reader;
   return reader+1;
}
//---------------------------------------------------------------------------
const char* TempFile::skipString(const char* reader)
   // Skip a string
{
   uint64_t rawLen;
   reader=readId(reader,rawLen);
   unsigned len=static_cast<unsigned>(rawLen);
   return reader+len;
}
//---------------------------------------------------------------------------
const char* TempFile::readId(const char* reader,uint64_t& id)
   // Read an id
{
   unsigned shift=0;
   id=0;
   while (true) {
      unsigned char c=*reinterpret_cast<const unsigned char*>(reader++);
      if (c&128) {
         id|=static_cast<uint64_t>(c&0x7F)<<shift;
         shift+=7;
      } else {
         id|=static_cast<uint64_t>(c)<<shift;
         break;
      }
   }
   return reader;
}
//---------------------------------------------------------------------------
const char* TempFile::readString(const char* reader,unsigned& len,const char*& str)
   // Read a string
{
   uint64_t rawLen;
   reader=readId(reader,rawLen);
   len=static_cast<unsigned>(rawLen);
   str=reader;
   return reader+len;
}
//---------------------------------------------------------------------------
