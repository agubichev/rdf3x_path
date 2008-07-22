#ifndef H_tools_rdf3xload_Sorter
#define H_tools_rdf3xload_Sorter
//---------------------------------------------------------------------------
class TempFile;
//---------------------------------------------------------------------------
/// Sort a temporary file
class Sorter {
   public:
   /// Sort a file
   static void sort(TempFile& in,TempFile& out,const char* (*skip)(const char*),int (*compare)(const char*,const char*),bool eliminateDuplicates=false);
};
//---------------------------------------------------------------------------
#endif
