package spark.compiletime

enum LogPlan:
  case Yes, No

object LogPlan:
  transparent inline given LogPlan = LogPlan.No
