package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static io.unitycatalog.server.utils.TestUtils.createApiClient;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.provider.Arguments;

public class ManagedTableReadWriteTest extends BaseTableReadWriteTest {

  private static final String ANOTHER_PARQUET_TABLE = "test_parquet_another";
  private static final String PARQUET_TABLE_PARTITIONED = "test_parquet_partitioned";
  private static final String DELTA_TABLE = "test_delta";
  private static final String PARQUET_TABLE = "test_parquet";
  private static final String ANOTHER_DELTA_TABLE = "test_delta_another";
  private static final String DELTA_TABLE_PARTITIONED = "test_delta_partitioned";
  private static final String CATALOG_OWNED_PROPERTY_CLAUSE =
      "TBLPROPERTIES('delta.feature.catalogOwned-preview'='supported')";

  private final File dataDir = new File(serverProperties.getProperty());

  private TableOperations tableOperations;

  protected static Stream<Arguments> cloudParameters() {
    return null;
  }

  private void setupManagedDeltaTable(
      String catalogName, String tableName, List<String> partitionColumns, SparkSession session)
      throws IOException, ApiException {
    String partitionClause;
    if (partitionColumns.isEmpty()) {
      partitionClause = "";
    } else {
      partitionClause = String.format(" PARTITIONED BY (%s)", String.join(", ", partitionColumns));
    }
    session.sql(
        String.format(
            "CREATE TABLE %s.%s.%s(i INT, s STRING) USING delta %s %s",
            catalogName, SCHEMA_NAME, tableName, CATALOG_OWNED_PROPERTY_CLAUSE, partitionClause));
  }

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    tableOperations = new SdkTableOperations(createApiClient(serverConfig));
  }

  @Override
  public void cleanUp() {
    super.cleanUp();
    try {
      JavaUtils.deleteRecursively(dataDir);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
