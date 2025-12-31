package io.unitycatalog.server.service.iceberg;

import java.util.Map;
import org.apache.iceberg.TableMetadata;

public class TableConfigService {
  private final FileIOFactory fileIOFactory;

  public TableConfigService(FileIOFactory fileIOFactory) {
    this.fileIOFactory = fileIOFactory;
  }

  public Map<String, String> getTableConfig(TableMetadata tableMetadata) {
    // TODO: metadataService.readTableMetadata called fileIOFactory.getFileIO already. It already
    //  generated this config but not passed back. For best efficiency the result from
    //  readTableMetadata should be reused.
    return fileIOFactory.getFileIOConfig(tableMetadata.location());
  }
}
