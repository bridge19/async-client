package io.bridge.webflux.client;

import feign.MethodMetadata;
import feign.Request;
import feign.Response;
import feign.reactor.client.ReactiveHttpClient;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpHeaders;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static reactor.core.publisher.Mono.just;

public class ReactiveWebFluxClient implements ReactiveHttpClient {

  private final static WebClient webClient = WebClient.builder().exchangeStrategies(ExchangeStrategies.builder()
      .codecs(configurer -> configurer
          .defaultCodecs()
          .maxInMemorySize(10 * 1024 * 1024))
      .build())
      .build();
  private final Type returnType;

  public ReactiveWebFluxClient(MethodMetadata methodMetadata) {
    this.returnType = methodMetadata.returnType();
  }

  @Override
  public Mono<Response> executeRequest(Mono<Request> requestMono) {
    return requestMono.flatMap(request -> {

      WebClient.RequestHeadersSpec requestHeadersSpec = null;
      if (request.method().equalsIgnoreCase("get")) {
        requestHeadersSpec = webClient.get().uri(request.url());
      } else if (request.method().equalsIgnoreCase("post")) {
        requestHeadersSpec = webClient.post().uri(request.url()).bodyValue(request.body());
      } else if (request.method().equalsIgnoreCase("put")) {
        requestHeadersSpec = webClient.put().uri(request.url()).bodyValue(request.body());
      } else if (request.method().equalsIgnoreCase("delete")) {
        requestHeadersSpec = webClient.delete().uri(request.url());
      } else if (request.method().equalsIgnoreCase("option")) {
        requestHeadersSpec = webClient.options().uri(request.url());
      } else if (request.method().equalsIgnoreCase("patch")) {
        requestHeadersSpec = webClient.patch().uri(request.url());
      } else if (request.method().equalsIgnoreCase("head")) {
        requestHeadersSpec = webClient.head().uri(request.url());
      } else {
        throw new RuntimeException("not support method: " + request.method());
      }
      if (request.headers() != null && request.headers().size() > 0) {
        Map<String,List<String>> headerMap = request.headers().entrySet().stream().collect(Collectors.toMap(entry-> entry.getKey(), entry -> new ArrayList<String>(entry.getValue())));
        requestHeadersSpec = requestHeadersSpec.headers((Consumer<HttpHeaders>) httpHeaders -> {
          httpHeaders.putAll(headerMap);
        });
      }
      return requestHeadersSpec.exchangeToMono((Function<ClientResponse, Mono<Response>>) clientResponse -> {
        Response.Builder responseBuilder = Response.builder();
        responseBuilder.status(clientResponse.rawStatusCode())
            .headers(clientResponse.headers().asHttpHeaders().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
            .request(request);
        return clientResponse.bodyToMono(ByteArrayResource.class).map(ByteArrayResource::getByteArray).defaultIfEmpty(new byte[0])
            .flatMap(bytesBody -> {
              responseBuilder.body(bytesBody);
              return just(responseBuilder.build());
            });
      }).onErrorMap((Predicate<? super Throwable>) ex -> ex instanceof ResourceAccessException && ex.getCause() instanceof SocketTimeoutException,
          (Function<? super Throwable, ? extends IOException>) ex -> new IOException(ex));
    });
}

//private static class WebFluxReactiveHttpResponse implements ReactiveHttpResponse {
//
//  private Type returnType;
//
//  private Map<String, List<String>> headers;
//  private Mono<byte[]> body;
//  private int status;
//
//  public WebFluxReactiveHttpResponse(Type returnType) {
//    this.returnType = returnType;
//  }
//
//  public void status(int status) {
//    this.status = status;
//  }
//
//  @Override
//  public int status() {
//    return this.status;
//  }
//
//  public void headers(Map<String, List<String>> headers) {
//    this.headers = headers;
//  }
//
//  @Override
//  public Map<String, List<String>> headers() {
//    return headers;
//  }
//
//  public void body(Mono<byte[]> body) {
//    this.body = body;
//  }
//
//  @Override
//  public Mono<byte[]> body() {
//    return body;
//  }
//
//  @Override
//  public Mono<byte[]> bodyData() {
//    return Mono.just(new byte[0]);
//  }
//}
//
//private static class ErrorReactiveHttpResponse implements ReactiveHttpResponse {
//
//  private final HttpStatusCodeException ex;
//
//  private ErrorReactiveHttpResponse(HttpStatusCodeException ex) {
//    this.ex = ex;
//  }
//
//  @Override
//  public int status() {
//    return ex.getStatusCode().value();
//  }
//
//  @Override
//  public Map<String, List<String>> headers() {
//    return ex.getResponseHeaders();
//  }
//
//  @Override
//  public Mono<byte[]> body() {
//    return Mono.empty();
//  }
//
//  @Override
//  public Mono<byte[]> bodyData() {
//    return Mono.just(ex.getResponseBodyAsByteArray());
//  }
//
//}

  private static Type ofPublisherType(Type returnType) {
    if (returnType instanceof ParameterizedType) {
      Type rawType = ((ParameterizedType) returnType).getRawType();
      if (rawType == Mono.class || rawType == Flux.class) {
        return rawType;
      }
    }
    return null;
  }

  private static Type ofReturnActualTypeType(Type returnType) {
    Type publishType = ofPublisherType(returnType);
    if (publishType != null) {
      return ((ParameterizedType) returnType).getActualTypeArguments()[0];
    }
    return returnType;
  }
}
