#include "cts/plangen/Plan.hpp"
//---------------------------------------------------------------------------
PlanContainer::PlanContainer()
   // Constructor
{
}
//---------------------------------------------------------------------------
PlanContainer::~PlanContainer()
   // Destructor
{
}
//---------------------------------------------------------------------------
void PlanContainer::clear()
   // Release all plans
{
   pool.freeAll();
}
//---------------------------------------------------------------------------