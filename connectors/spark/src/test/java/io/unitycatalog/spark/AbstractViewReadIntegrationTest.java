package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME2;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME2;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end guarantee that a Unity Catalog view is resolvable and readable through a real Spark
 * session on every supported Spark version (the mock-based {@code UCViewProxySuite} / {@code
 * UCViewReadOnlySuite} don't drive Spark's analyzer, so they miss view-resolution bugs). All tests
 * live here; subclasses supply only the version-appropriate {@link #createView()} / {@link
 * #dropView()} (SQL on Spark 4.2, the SDK on 4.0/4.1, which lack the v2 ViewCatalog).
 *
 * <p>The shared fixture is a deliberately demanding view -- a literal plus a join of two tables in
 * DIFFERENT catalogs, one referenced UNQUALIFIED and one FULLY-QUALIFIED, with a filter ({@link
 * #VIEW_QUERY}) -- whose declared column names ({@link #DECLARED_COLUMNS}) differ from the names
 * its SELECT list produces ({@link #QUERY_OUTPUT_COLUMNS}). Created while the current namespace is
 * {@code CATALOG_NAME.SCHEMA_NAME} but read back with a different current catalog, it exercises
 * both view creation-context round-trips at once, on whichever create path the subclass uses:
 *
 * <ul>
 *   <li>the unqualified {@code employees} can only resolve against the view's captured creation
 *       catalog/namespace ({@code view.catalogAndNamespace.*}) -- the current catalog at read time
 *       is elsewhere and has no such table;
 *   <li>the declared columns differ from the query output, so reading them requires the persisted
 *       query-output names ({@code view.query.out.*}).
 * </ul>
 *
 * If the connector substituted the view's own location for the creation context, or regenerated the
 * query-output names from the declared columns, reads fail with {@code TABLE_OR_VIEW_NOT_FOUND} /
 * {@code INCOMPATIBLE_VIEW_SCHEMA_CHANGE}.
 */
public abstract class AbstractViewReadIntegrationTest extends BaseSparkIntegrationTest {

  /** Location for the external Delta table; the managed table needs no explicit location. */
  @TempDir protected File departmentsDir;

  protected static final String VIEW_NAME = "spark_test_view";
  protected static final String VIEW_FULL_NAME = CATALOG_NAME + "." + SCHEMA_NAME + "." + VIEW_NAME;

  /** A MANAGED Delta table (in CATALOG_NAME.SCHEMA_NAME), referenced UNQUALIFIED in the view. */
  protected static final String EMPLOYEES_FULL_NAME =
      CATALOG_NAME + "." + SCHEMA_NAME + ".employees";
  /**
   * An EXTERNAL Delta table in a different catalog AND a different schema ({@code
   * CATALOG_NAME2.SCHEMA_NAME2}), referenced FULLY-QUALIFIED.
   */
  protected static final String DEPARTMENTS_FULL_NAME =
      CATALOG_NAME2 + "." + SCHEMA_NAME2 + ".departments";

  /**
   * Names the view's SELECT list produces -- a mix of a literal ({@code 123}), an explicit alias
   * ({@code e_id}, so the query-output name differs from the source column {@code id}), and a
   * passthrough ({@code budget}). All differ from {@link #DECLARED_COLUMNS}.
   */
  protected static final String[] QUERY_OUTPUT_COLUMNS = {"123", "e_id", "budget"};
  /** Declared view column names -- intentionally different from {@link #QUERY_OUTPUT_COLUMNS}. */
  protected static final String[] DECLARED_COLUMNS = {"num", "emp", "dept_budget"};

  protected static final String VIEW_QUERY =
      "SELECT 123, e.id AS e_id, d.budget FROM employees e "
          + "JOIN "
          + DEPARTMENTS_FULL_NAME
          + " d ON e.dept_id = d.dept_id WHERE d.budget > 100";

  /** Creates VIEW_NAME (declaring {@link #DECLARED_COLUMNS} over {@link #VIEW_QUERY}). */
  protected abstract void createView();

  /** Drops VIEW_NAME. */
  protected abstract void dropView();

  /**
   * Asserts the view is (or, when {@code expectPresent} is false, is not) listed by {@code SHOW
   * VIEWS}. Default no-op: {@code SHOW VIEWS} against a v2 catalog only works on Spark 4.2 (on
   * 4.0/4.1 it throws {@code missingCatalogViewsAbilityError}, like {@code CREATE VIEW}); the 4.2
   * subclass overrides this.
   */
  protected void verifyShowViews(boolean expectPresent) {}

  /**
   * Table DDL (unlike view DDL) is not version-gated, so this works on every Spark version. {@code
   * employees} is a MANAGED Delta table (UC assigns its location from the server's managed storage
   * root); {@code departments} is an EXTERNAL Delta table at an explicit temp location.
   */
  protected void createSourceTables() {
    sql("CREATE TABLE %s (id INT, dept_id INT) USING delta", EMPLOYEES_FULL_NAME);
    sql("INSERT INTO %s VALUES (1, 10), (2, 20), (3, 10), (4, 99)", EMPLOYEES_FULL_NAME);
    sql("CREATE SCHEMA IF NOT EXISTS %s.%s", CATALOG_NAME2, SCHEMA_NAME2);
    sql(
        "CREATE TABLE %s (dept_id INT, budget INT) USING delta LOCATION '%s'",
        DEPARTMENTS_FULL_NAME, departmentsDir.toURI());
    sql("INSERT INTO %s VALUES (10, 500), (20, 50), (30, 700)", DEPARTMENTS_FULL_NAME);
  }

  protected void createSessionAndView() {
    session = createSparkSessionWithCatalogs(SPARK_CATALOG, CATALOG_NAME, CATALOG_NAME2);
    createSourceTables();
    // Create the view while the current namespace is CATALOG_NAME.SCHEMA_NAME, so the unqualified
    // `employees` binds there and that becomes the view's captured creation catalog/namespace.
    sql("USE %s.%s", CATALOG_NAME, SCHEMA_NAME);
    createView();
  }

  /**
   * Asserts VIEW_NAME's presence in {@code SHOW TABLES} (which lists views on every version), using
   * the fully-qualified {@code IN catalog.schema} form. When {@code currentIsViewNamespace} is true
   * (the session's current catalog/namespace is the view's own), also checks the unqualified {@code
   * SHOW TABLES}, which lists the current namespace.
   */
  protected void verifyShowTables(boolean expectPresent, boolean currentIsViewNamespace) {
    assertThat(viewListedBy(sql("SHOW TABLES IN %s.%s", CATALOG_NAME, SCHEMA_NAME)))
        .isEqualTo(expectPresent);
    if (currentIsViewNamespace) {
      assertThat(viewListedBy(sql("SHOW TABLES"))).isEqualTo(expectPresent);
    }
  }

  private static boolean viewListedBy(List<Row> showResult) {
    return showResult.stream().anyMatch(r -> VIEW_NAME.equals(r.getString(1)));
  }

  /**
   * SELECT / DESCRIBE the view under its DECLARED column names, referencing it by {@code viewRef}.
   * The result is the same no matter what catalog is current, because the view's persisted creation
   * context ({@code view.catalogAndNamespace.*} for the unqualified {@code employees}, {@code
   * view.query.out.*} for the declared columns) does not depend on it. The filter keeps budget >
   * 100 (drops dept 20) and the inner join drops employee 4 (dept 99, unmatched).
   */
  private void verifyViewReadable(String viewRef) {
    assertThat(
            sql(
                    "SELECT %s, %s, %s FROM %s ORDER BY %s",
                    DECLARED_COLUMNS[0],
                    DECLARED_COLUMNS[1],
                    DECLARED_COLUMNS[2],
                    viewRef,
                    DECLARED_COLUMNS[1])
                .stream()
                .map(r -> r.getInt(0) + ":" + r.getInt(1) + ":" + r.getInt(2))
                .collect(Collectors.toList()))
        .containsExactly("123:1:500", "123:3:500");

    assertThat(sql("DESCRIBE %s", viewRef).stream().map(r -> r.getString(0)))
        .contains(DECLARED_COLUMNS[0], DECLARED_COLUMNS[1], DECLARED_COLUMNS[2]);
  }

  /**
   * All view assertions share one Spark session + fixture (creating a session and Delta tables is
   * expensive), so they run as ordered stages in a single test. SHOW / SELECT / DESCRIBE run twice
   * -- once with the view's own namespace current, once with a different catalog+schema current --
   * to prove resolution is catalog-independent from both vantage points. Drop runs last since it
   * destroys the view.
   */
  @Test
  public void testViewLifecycleThroughSpark() {
    createSessionAndView();

    // View's own namespace current: it owns the view and the unqualified `employees`, so the view
    // is referenced by its BARE name -- proving it resolves unqualified against the current ns.
    sql("USE %s.%s", CATALOG_NAME, SCHEMA_NAME);
    verifyShowTables(true, /* currentIsViewNamespace= */ true);
    verifyShowViews(true);
    verifyViewReadable(VIEW_NAME);

    // A different catalog AND schema current: owns `departments` but not `employees` or the view.
    sql("USE %s.%s", CATALOG_NAME2, SCHEMA_NAME2);
    verifyShowTables(true, /* currentIsViewNamespace= */ false);
    verifyShowViews(true);
    verifyViewReadable(VIEW_FULL_NAME);

    // Drop last: it destroys the view, so no assertion after this depends on it existing.
    dropView();
    verifyShowTables(false, /* currentIsViewNamespace= */ false);
    verifyShowViews(false);
  }
}
