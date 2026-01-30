package io.unitycatalog.server.service.deltarest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.annotation.ExceptionHandlerFunction;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import java.util.HashMap;
import java.util.Map;
import lombok.SneakyThrows;

/**
 * Exception handler for Delta REST Catalog API.
 *
 * <p>Converts exceptions to JSON error responses following the Delta REST Catalog error format.
 */
public class DeltaRestExceptionHandler implements ExceptionHandlerFunction {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override
  public HttpResponse handleException(ServiceRequestContext ctx, HttpRequest req, Throwable cause) {
    try {
      if (cause instanceof BaseException baseException) {
        return handleBaseException(baseException);
      } else if (cause instanceof IllegalArgumentException) {
        return createErrorResponse(
            HttpStatus.BAD_REQUEST, "BadRequestException", cause.getMessage());
      } else if (cause instanceof SecurityException) {
        return createErrorResponse(
            HttpStatus.FORBIDDEN, "ForbiddenException", cause.getMessage());
      } else {
        return createErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR,
            cause.getClass().getSimpleName(), cause.getMessage());
      }
    } catch (Exception e) {
      return HttpResponse.of(HttpStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private HttpResponse handleBaseException(BaseException exception) {
    HttpStatus status = exception.getErrorCode().getHttpStatus();
    String errorType = mapErrorCodeToType(exception.getErrorCode());
    return createErrorResponse(status, errorType, exception.getMessage());
  }

  private String mapErrorCodeToType(ErrorCode errorCode) {
    return switch (errorCode) {
      case NOT_FOUND -> "NoSuchEntityException";
      case ALREADY_EXISTS -> "AlreadyExistsException";
      case INVALID_ARGUMENT -> "BadRequestException";
      case PERMISSION_DENIED -> "ForbiddenException";
      case UNAUTHENTICATED -> "UnauthorizedException";
      case FAILED_PRECONDITION -> "PreconditionFailedException";
      case RESOURCE_EXHAUSTED -> "ResourceExhaustedException";
      case ABORTED -> "ConflictException";
      case UNIMPLEMENTED -> "NotImplementedException";
      case INTERNAL -> "InternalServerErrorException";
      case DATA_LOSS -> "DataLossException";
      default -> "UnknownException";
    };
  }

  @SneakyThrows
  private HttpResponse createErrorResponse(HttpStatus status, String errorType, String message) {
    Map<String, Object> error = new HashMap<>();
    error.put("error", Map.of(
        "code", status.code(),
        "type", errorType,
        "message", message != null ? message : "Unknown error"
    ));

    return HttpResponse.of(
        status,
        MediaType.JSON,
        MAPPER.writeValueAsString(error));
  }
}
