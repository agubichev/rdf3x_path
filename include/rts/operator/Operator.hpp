#ifndef H_rts_operator_Operator
#define H_rts_operator_Operator
//---------------------------------------------------------------------------
class Register;
//---------------------------------------------------------------------------
/// Base class for all operators of the runtime system
class Operator
{
   protected:
   /// Helper for indenting debug output
   static void indent(unsigned level);
   /// Helper for debug output
   static void printRegister(const Register* reg);

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
   virtual void print(unsigned indent=0) = 0;
};
//---------------------------------------------------------------------------
#endif
