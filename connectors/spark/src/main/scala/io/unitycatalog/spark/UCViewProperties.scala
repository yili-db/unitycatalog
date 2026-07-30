package io.unitycatalog.spark

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.catalog.CatalogTable

/**
 * (De)serialization of Spark's internal `view.*` metadata to/from a UC properties map.
 *
 * Spark's v2 `View` exposes the resolution-critical view metadata (SQL configs, query-output
 * column names, creation catalog/namespace, ...) through typed accessors, but persists it as a flat
 * bag of `view.*` string properties (`CatalogTable.VIEW_*`). UC has no first-class fields for these,
 * so the connector round-trips them through the generic properties map. This object owns that
 * encoding in one place, kept free of Spark-4.2-only types so it compiles on all supported Spark
 * versions and is shared by both the 4.2 (`toView`) and 4.0/4.1 (`buildV1ViewTable`) load paths.
 *
 * Distinct from [[UCViewTypes]], which owns the UC-`TableType`-to-Spark-view-kind mapping.
 */
private[spark] object UCViewProperties {

  /**
   * Splits the `VIEW_SQL_CONFIG_PREFIX`-prefixed entries out of a UC properties map and returns
   * them un-prefixed, as Spark's `View.sqlConfigs()` expects.
   */
  def extractSqlConfigs(properties: util.Map[String, String]): util.Map[String, String] = {
    properties.asScala.collect {
      case (k, v) if k.startsWith(CatalogTable.VIEW_SQL_CONFIG_PREFIX) =>
        k.substring(CatalogTable.VIEW_SQL_CONFIG_PREFIX.length) -> v
    }.toMap.asJava
  }

  /**
   * Inverse of [[extractQueryColumnNames]]: encodes the view's query-output column names as the
   * `view.query.out.numCols` + `view.query.out.col.<i>` properties Spark expects, mirroring its own
   * v1 `CatalogTable` encoding. Empty input yields an empty map (nothing to persist). Shared by the
   * create path (Spark 4.2, from `View.queryColumnNames()`) and the 4.0/4.1 read path (synthesizing
   * from the declared columns when the row never persisted them).
   */
  def queryColumnNamesToProps(queryColumnNames: Seq[String]): Map[String, String] = {
    if (queryColumnNames.isEmpty) {
      Map.empty
    } else {
      Map(CatalogTable.VIEW_QUERY_OUTPUT_NUM_COLUMNS -> queryColumnNames.length.toString) ++
        queryColumnNames.zipWithIndex.map { case (name, i) =>
          s"${CatalogTable.VIEW_QUERY_OUTPUT_COLUMN_NAME_PREFIX}$i" -> name
        }
    }
  }

  /**
   * Reads the persisted query-output column names of a view -- the names the view's SELECT list
   * produces, which Spark uses to match the parsed query output against the declared view schema.
   * Stored as `view.query.out.numCols` plus `view.query.out.col.<i>` (`VIEW_QUERY_OUTPUT_*`);
   * returns them in ordinal order, or `None` when the count key is absent (a row that never
   * persisted them).
   */
  def extractQueryColumnNames(properties: util.Map[String, String]): Option[Seq[String]] = {
    Option(properties.get(CatalogTable.VIEW_QUERY_OUTPUT_NUM_COLUMNS)).map { numColsStr =>
      val numCols = numColsStr.toInt
      (0 until numCols).map { i =>
        val key = CatalogTable.VIEW_QUERY_OUTPUT_COLUMN_NAME_PREFIX + i
        Option(properties.get(key)).getOrElse(
          throw new IllegalStateException(
            s"Corrupted view metadata: expected $numCols query-output columns but $key is missing"))
      }
    }
  }

  /**
   * Reads the current catalog + namespace captured when the view was created, against which
   * unqualified table references in the view body are resolved. Stored as
   * `view.catalogAndNamespace.numParts` plus `view.catalogAndNamespace.part.<i>` (part 0 is the
   * catalog, the rest the namespace). Returns `(catalog, namespace)`, or `None` when the count key
   * is absent (a row that never persisted the resolution context).
   */
  def extractCatalogAndNamespace(
      properties: util.Map[String, String]): Option[(String, Seq[String])] = {
    Option(properties.get(CatalogTable.VIEW_CATALOG_AND_NAMESPACE)).flatMap { numPartsStr =>
      val parts = (0 until numPartsStr.toInt).map { i =>
        val key = CatalogTable.VIEW_CATALOG_AND_NAMESPACE_PART_PREFIX + i
        Option(properties.get(key)).getOrElse(
          throw new IllegalStateException(
            s"Corrupted view metadata: expected ${numPartsStr.toInt} catalog/namespace parts " +
              s"but $key is missing"))
      }
      // part 0 is the catalog; the remainder is the namespace. A degenerate 0-part list has no
      // catalog to resolve against, so treat it as absent.
      parts.headOption.map(catalog => (catalog, parts.tail))
    }
  }
}
