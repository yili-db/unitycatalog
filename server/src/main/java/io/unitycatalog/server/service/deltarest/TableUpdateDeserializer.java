package io.unitycatalog.server.service.deltarest;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.server.model.deltarest.AddCommitUpdate;
import io.unitycatalog.server.model.deltarest.RemovePropertiesUpdate;
import io.unitycatalog.server.model.deltarest.SetLatestBackfilledVersionUpdate;
import io.unitycatalog.server.model.deltarest.SetPropertiesUpdate;
import io.unitycatalog.server.model.deltarest.SetSchemaUpdate;
import io.unitycatalog.server.model.deltarest.SetTableCommentUpdate;
import io.unitycatalog.server.model.deltarest.UpdateProtocolUpdate;
import java.io.IOException;

/**
 * Custom deserializer for TableUpdate that handles polymorphic deserialization based on the
 * "action" field. This is needed because the OpenAPI Generator (with library: "resteasy")
 * generates separate classes for each update type without creating an inheritance relationship,
 * even though they are defined as oneOf with a discriminator in the OpenAPI spec.
 */
public class TableUpdateDeserializer extends JsonDeserializer<Object> {

  @Override
  public Object deserialize(JsonParser jsonParser, DeserializationContext context)
      throws IOException {
    ObjectMapper mapper = (ObjectMapper) jsonParser.getCodec();
    JsonNode node = mapper.readTree(jsonParser);

    // Extract the "action" field to determine which type to deserialize
    JsonNode actionNode = node.get("action");
    if (actionNode == null) {
      throw new IOException("Missing required 'action' field in TableUpdate");
    }

    String action = actionNode.asText();

    // Deserialize based on the action type
    return switch (action) {
      case "delta-add-commit" -> mapper.treeToValue(node, AddCommitUpdate.class);
      case "delta-set-latest-backfilled-version" ->
          mapper.treeToValue(node, SetLatestBackfilledVersionUpdate.class);
      case "delta-set-schema-and-column-masks" -> mapper.treeToValue(node, SetSchemaUpdate.class);
      case "delta-set-table-comment" -> mapper.treeToValue(node, SetTableCommentUpdate.class);
      case "delta-update-protocol" -> mapper.treeToValue(node, UpdateProtocolUpdate.class);
      case "remove-properties" -> mapper.treeToValue(node, RemovePropertiesUpdate.class);
      case "set-properties" -> mapper.treeToValue(node, SetPropertiesUpdate.class);
      default -> throw new IOException("Unknown action type: " + action);
    };
  }
}
