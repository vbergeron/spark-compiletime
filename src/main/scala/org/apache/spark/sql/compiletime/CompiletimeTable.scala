package org.apache.spark.sql.compiletime

import org.apache.spark.sql.connector.catalog.*
import org.apache.spark.sql.types.StructType

class CompiletimeTable(val ident: Identifier, val schema: StructType) extends Table {
  override def name(): String = ident.name()

  override def capabilities(): java.util.Set[TableCapability] = java.util.Collections.emptySet()
}
