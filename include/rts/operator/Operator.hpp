#ifndef H_rts_operator_Operator
#define H_rts_operator_Operator
//---------------------------------------------------------------------------
/// Base class for all operators of the runtime system
class Operator
{
   protected:
   /// Helper for indenting debug output
   static void indent(unsigned level);

   public:
   /// Constructor
   Operator();
   /// Destructor
   virtual ~Operator();

   /// Produce the first tuple
   virtual bool first() = 0;
   /// Produce the next tuple
   virtual bool next() = 0;

   /// Print the operator tree. Debugging only.
   virtual void print(unsigned indent) = 0;
};
//---------------------------------------------------------------------------
#endif
