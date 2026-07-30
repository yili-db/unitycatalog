package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static io.unitycatalog.server.utils.TestUtils.createApiClient;

import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;

/**
 * Spark 4.0/4.1 lack the v2 {@code ViewCatalog}, so views can't be created via SQL DDL. All
 * assertions live in {@link AbstractViewReadIntegrationTest}; this subclass only supplies the
 * server-side (SDK) create/drop. The created view carries the same declared-vs-query-output column
 * difference and creation catalog/namespace as the SQL path, so the shared tests exercise the
 * {@code view.query.out.*} and {@code view.catalogAndNamespace.*} round-trips on the read-only
 * {@code buildV1ViewTable} path too.
 */
public class UCViewReadOnlyIntegrationTest extends AbstractViewReadIntegrationTest {

  @Override
  @SneakyThrows
  protected void createView() {
    new SdkTableOperations(createApiClient(serverConfig))
        .createTable(
            new CreateTable()
                .name(VIEW_NAME)
                .catalogName(CATALOG_NAME)
                .schemaName(SCHEMA_NAME)
                .tableType(TableType.VIEW)
                .viewDefinition(VIEW_QUERY)
                // Omit view_dependencies (like the connector does for a plain view); the server
                // accepts an absent list.
                .properties(
                    Map.of(
                        // Query-output names (differ from the declared columns below).
                        "view.query.out.numCols", Integer.toString(QUERY_OUTPUT_COLUMNS.length),
                        "view.query.out.col.0", QUERY_OUTPUT_COLUMNS[0],
                        "view.query.out.col.1", QUERY_OUTPUT_COLUMNS[1],
                        "view.query.out.col.2", QUERY_OUTPUT_COLUMNS[2],
                        // Creation catalog/namespace, so the unqualified `employees` resolves.
                        "view.catalogAndNamespace.numParts", "2",
                        "view.catalogAndNamespace.part.0", CATALOG_NAME,
                        "view.catalogAndNamespace.part.1", SCHEMA_NAME))
                .columns(
                    List.of(
                        intColumn(DECLARED_COLUMNS[0], 0),
                        intColumn(DECLARED_COLUMNS[1], 1),
                        intColumn(DECLARED_COLUMNS[2], 2))));
  }

  @Override
  @SneakyThrows
  protected void dropView() {
    new SdkTableOperations(createApiClient(serverConfig)).deleteTable(VIEW_FULL_NAME);
  }

  private static ColumnInfo intColumn(String name, int position) {
    return new ColumnInfo()
        .name(name)
        .typeName(ColumnTypeName.INT)
        .typeText("int")
        .typeJson(
            "{\"name\":\"" + name + "\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}")
        .nullable(true)
        .position(position);
  }
}
