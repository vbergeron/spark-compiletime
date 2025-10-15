package org.apache.spark.sql.compiletime

import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.catalyst.CurrentUserContext

final case class CompiletimeOptimizer(manager: CatalogManager) extends Optimizer(manager) {
  CurrentUserContext.CURRENT_USER.set("compiletime")
}
