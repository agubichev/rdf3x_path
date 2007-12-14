#ifndef H_rts_runtime_Runtime
#define H_rts_runtime_Runtime
//---------------------------------------------------------------------------
#include <vector>
//---------------------------------------------------------------------------
class Database;
//---------------------------------------------------------------------------
/// A runtime register storing a single value
class Register
{
   public:
   /// The value
   unsigned value;
};
//---------------------------------------------------------------------------
/// The runtime system
class Runtime
{
   private:
   /// The database
   Database& db;
   /// The registers
   std::vector<Register> registers;

   public:
   /// Constructor
   explicit Runtime(Database& db);
   /// Destructor
   ~Runtime();

   /// Get the database
   Database& getDatabase() const { return db; }
   /// Set the number of registers
   void allocateRegisters(unsigned count);
   /// Access a specific register
   Register* getRegister(unsigned slot) { return &(registers[slot]); }
};
//---------------------------------------------------------------------------
#endif
