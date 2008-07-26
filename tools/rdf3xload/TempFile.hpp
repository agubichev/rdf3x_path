#ifndef H_tools_rdf3xload_TempFile
#define H_tools_rdf3xload_TempFile
//---------------------------------------------------------------------------
#include <fstream>
#include <string>
//---------------------------------------------------------------------------
#if defined(_MSC_VER)
typedef unsigned __int64 uint64_t;
#endif
//---------------------------------------------------------------------------
/// A temporary file
class TempFile
{
   private:
   /// The next id
   static unsigned id;

   /// The base file name
   std::string baseName;
   /// The file name
   std::string fileName;
   /// The output
   std::ofstream out;

   /// The buffer size
   static const unsigned bufferSize = 16384;
   /// The write buffer
   char writeBuffer[bufferSize];
   /// The write pointer
   unsigned writePointer;

   /// Construct a new suffix
   static std::string newSuffix();

   public:
   /// Constructor
   TempFile(const std::string& baseName);
   /// Destructor
   ~TempFile();

   /// Get the base file name
   const std::string& getBaseFile() const { return baseName; }
   /// Get the file name
   const std::string& getFile() const { return fileName; }

   /// Flush the file
   void flush();
   /// Close the file
   void close();
   /// Discard the file
   void discard();

   /// Write a string
   void writeString(unsigned len,const char* str);
   /// Write a id
   void writeId(uint64_t id);
   /// Raw write
   void write(unsigned len,const char* data);

   /// Skip an id
   static const char* skipId(const char* reader);
   /// Skip a string
   static const char* skipString(const char* reader);
   /// Read an id
   static const char* readId(const char* reader,uint64_t& id);
   /// Read a string
   static const char* readString(const char* reader,unsigned& len,const char*& str);
};
//---------------------------------------------------------------------------
#endif
