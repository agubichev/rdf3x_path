#ifndef H_rts_segment_DictionarySegment
#define H_rts_segment_DictionarySegment
//---------------------------------------------------------------------------
#include "rts/segment/Segment.hpp"
#include <string>
//---------------------------------------------------------------------------
/// A dictionary mapping strings to ids and backwards
class DictionarySegment : public Segment
{
   private:
   /// The start of the raw string table
   unsigned tableStart;
   /// The start of the mapping table (id->page)
   unsigned mappingStart;
   /// The root of the index b-tree
   unsigned indexRoot;

   /// Lookup an id for a given string on a certain page in the raw string table
   bool lookupOnPage(unsigned pageNo,const std::string& text,unsigned hash,unsigned& id);

   public:
   /// Constructor
   DictionarySegment(BufferManager& bufferManager,unsigned tableStart,unsigned mappingStart,unsigned indexRoot);

   /// Lookup an id for a given string
   bool lookup(const std::string& text,unsigned& id);
   /// Lookup a string for a given id
   bool lookupById(unsigned id,std::string& text);
};
//---------------------------------------------------------------------------
#endif
