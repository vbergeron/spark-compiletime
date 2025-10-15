package spark.compiletime

sealed trait ShowPlans

object ShowPlans:
  case object Yes extends ShowPlans
  case object No  extends ShowPlans

  inline def yes: Yes.type = Yes
  inline def no: No.type   = No

  transparent inline given ShowPlans = no
