package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Spark 4.2 exposes the v2 {@code ViewCatalog}, so view create/drop route to Unity Catalog through
 * SQL DDL. All assertions live in {@link AbstractViewReadIntegrationTest}; this subclass supplies
 * the SQL create/drop and the {@code SHOW VIEWS} check (a v2-{@code ViewCatalog}-only command).
 */
public class UCViewDDLIntegrationTest extends AbstractViewReadIntegrationTest {

  @Override
  protected void createView() {
    // Declare DECLARED_COLUMNS over VIEW_QUERY's output (QUERY_OUTPUT_COLUMNS); the names differ,
    // so this exercises the query-output-column round-trip.
    sql(
        "CREATE VIEW %s (%s, %s, %s) AS %s",
        VIEW_FULL_NAME,
        DECLARED_COLUMNS[0], DECLARED_COLUMNS[1], DECLARED_COLUMNS[2],
        VIEW_QUERY);
  }

  @Override
  protected void dropView() {
    sql("DROP VIEW %s", VIEW_FULL_NAME);
  }

  @Override
  protected void verifyShowViews(boolean expectPresent) {
    List<String> views =
        sql("SHOW VIEWS IN %s.%s", CATALOG_NAME, SCHEMA_NAME).stream()
            .map(r -> r.getString(1))
            .collect(Collectors.toList());
    if (expectPresent) {
      // SHOW VIEWS lists views only -- the view is present, the source Delta tables are not.
      assertThat(views).contains(VIEW_NAME).doesNotContain("employees");
    } else {
      assertThat(views).doesNotContain(VIEW_NAME);
    }
  }
}
