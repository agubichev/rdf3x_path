#ifndef H_rts_runtime_Runtime
#define H_rts_runtime_Runtime
//---------------------------------------------------------------------------
// RDF-3X
// (c) 2008 Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//---------------------------------------------------------------------------
#include "rts/runtime/DomainDescription.hpp"
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
   /// The potential domain (if known)
   PotentialDomainDescription* domain;

   /// Reset the register (both value and domain)
   void reset();
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
   /// The domain descriptions
   std::vector<PotentialDomainDescription> domainDescriptions;

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
   /// Set the number of domain descriptions
   void allocateDomainDescriptions(unsigned count);
   /// Access a specific domain description
   PotentialDomainDescription* getDomainDescription(unsigned slot) { return &(domainDescriptions[slot]); }
};
//---------------------------------------------------------------------------
#endif
