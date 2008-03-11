#include "rts/database/Database.hpp"
#include <iostream>
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
void buildAndShowStatistics(Database& db,Database::DataOrder order);
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   if (argc!=2) {
      cout << "usage: " << argv[0] << " <db>" << endl;
      return 1;
   }

   // Open the database
   Database db;
   if (!db.open(argv[1])) {
      cout << "Unable to open " << argv[1] << endl;
      return 1;
   }

   // Compute the statistics
   buildAndShowStatistics(db,Database::Order_Predicate_Object_Subject);
}
//---------------------------------------------------------------------------
