#include "TempFile.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
/// The next id
unsigned TempFile::id = 0;
//---------------------------------------------------------------------------
string TempFile::newSuffix()
   // Construct a new suffix
{
   char buffer[50];
   snprintf(buffer,sizeof(buffer),".%u",id++);
   return string(buffer);
}
//---------------------------------------------------------------------------
TempFile::TempFile(const string& baseName)
   : baseName(baseName),fileName(baseName+newSuffix()),out(fileName.c_str())
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
   out.write(str,len);
}
//---------------------------------------------------------------------------
void TempFile::writeId(uint64_t id)
   // Write a id
{
   while (id>=128) {
      unsigned char c=static_cast<unsigned char>(id|128);
      out.put(c);
      id>>=7;
   }
   out.put(static_cast<unsigned char>(id));
}
//---------------------------------------------------------------------------
void TempFile::write(unsigned len,const char* data)
   // Raw write
{
   out.write(data,len);
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
