package spark.compiletime

enum ShowPlans:
  case Yes, No

object ShowPlans:
  transparent inline given ShowPlans = ShowPlans.No
