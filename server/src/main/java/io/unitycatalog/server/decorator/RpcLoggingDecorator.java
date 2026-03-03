package io.unitycatalog.server.decorator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.DecoratingHttpServiceFunction;
import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.ServiceRequestContext;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decorator that logs all RPC requests and responses at a centralized point.
 *
 * <p>This decorator intercepts all HTTP requests and responses passing through the service layer
 * and logs them using error level logging to ensure they are always captured.
 */
public class RpcLoggingDecorator implements DecoratingHttpServiceFunction {

  private static final Logger LOGGER = LoggerFactory.getLogger(RpcLoggingDecorator.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Override
  public HttpResponse serve(HttpService delegate, ServiceRequestContext ctx, HttpRequest req)
      throws Exception {

    // Aggregate the request once and log it, then pass it to the delegate
    return HttpResponse.of(
        req.aggregate()
            .thenCompose(
                aggregatedRequest -> {
                  // Log the request
                  logRequest(ctx, req, aggregatedRequest.contentUtf8());

                  // Convert back to HttpRequest and pass to delegate
                  HttpRequest newReq = HttpRequest.of(req.headers(), aggregatedRequest.content());

                  // Get the response from the delegate service
                  HttpResponse response;
                  try {
                    response = delegate.serve(ctx, newReq);
                  } catch (Exception e) {
                    LOGGER.error(
                        "\n[RPC Req] {} {}, Error in delegate: {}",
                        req.method(),
                        req.path(),
                        e.getMessage(),
                        e);
                    return CompletableFuture.completedFuture(
                        HttpResponse.of(HttpStatus.INTERNAL_SERVER_ERROR));
                  }

                  // Log the response
                  return response
                      .aggregate()
                      .thenApply(
                          aggregatedResponse -> {
                            logResponse(
                                ctx, aggregatedResponse.status(), aggregatedResponse.contentUtf8());
                            return aggregatedResponse.toHttpResponse();
                          });
                })
            .exceptionally(
                throwable -> {
                  LOGGER.error(
                      "\n[RPC Error] {} {}, Error: {}",
                      req.method(),
                      req.path(),
                      throwable.getMessage(),
                      throwable);
                  return HttpResponse.of(HttpStatus.INTERNAL_SERVER_ERROR);
                }));
  }

  private void logRequest(ServiceRequestContext ctx, HttpRequest req, String requestBody) {
    try {
      String prettyRequestBody = requestBody;

      // Pretty print JSON if possible
      if (!requestBody.isEmpty()) {
        String contentType = req.headers().get("Content-Type", "application/json");
        if (contentType.contains("application/json")) {
          try {
            Object json = OBJECT_MAPPER.readValue(requestBody, Object.class);
            prettyRequestBody =
                OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(json);
          } catch (Exception e) {
            // If parsing fails, use original body
          }
        }
      }

      LOGGER.error(
          "\n[RPC Req] {} {}, Headers: {}, Body: {}",
          req.method(),
          req.path(),
          req.headers(),
          prettyRequestBody.isEmpty() ? "<empty>" : "\n" + prettyRequestBody);
    } catch (Exception e) {
      LOGGER.error("\n[RPC Req] {} {}, Error: {}", req.method(), req.path(), e.getMessage(), e);
    }
  }

  private void logResponse(ServiceRequestContext ctx, HttpStatus status, String responseBody) {
    try {
      String prettyResponseBody = responseBody;

      // Pretty print JSON if possible
      if (!responseBody.isEmpty()) {
        try {
          Object json = OBJECT_MAPPER.readValue(responseBody, Object.class);
          prettyResponseBody =
              OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(json);
        } catch (Exception e) {
          // If parsing fails, use original body
        }
      }

      LOGGER.error(
          "\n[RPC Resp] {} {}, Status: {}, Body: {}",
          ctx.request().method(),
          ctx.request().path(),
          status,
          prettyResponseBody.isEmpty() ? "<empty>" : "\n" + prettyResponseBody);
    } catch (Exception e) {
      LOGGER.error(
          "\n[RPC Resp] {} {}, Status: {}, Error: {}",
          ctx.request().method(),
          ctx.request().path(),
          status,
          e.getMessage(),
          e);
    }
  }
}
