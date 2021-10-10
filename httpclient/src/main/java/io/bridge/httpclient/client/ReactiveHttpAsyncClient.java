package io.bridge.httpclient.client;

import feign.MethodMetadata;
import feign.Request;
import feign.Response;
import feign.reactor.client.ReactiveHttpClient;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.*;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.http.util.EntityUtils;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ReactiveHttpAsyncClient implements ReactiveHttpClient {

  private static final CloseableHttpAsyncClient client;

  static {
    RequestConfig requestConfig = RequestConfig.custom()
        .setConnectTimeout(15000)
        .setSocketTimeout(15000)
        .setConnectionRequestTimeout(1000)
        .build();

    //配置io线程
    IOReactorConfig ioReactorConfig = IOReactorConfig.custom().
        setIoThreadCount(Runtime.getRuntime().availableProcessors())
        .setSoKeepAlive(true)
        .build();
    //设置连接池大小
    ConnectingIOReactor ioReactor = null;
    try {
      ioReactor = new DefaultConnectingIOReactor(ioReactorConfig);
    } catch (IOReactorException e) {
      e.printStackTrace();
    }
    PoolingNHttpClientConnectionManager connManager = new PoolingNHttpClientConnectionManager(ioReactor);
    connManager.setMaxTotal(100);
    connManager.setDefaultMaxPerRoute(100);


    client = HttpAsyncClients.custom().
        setConnectionManager(connManager)
        .setDefaultRequestConfig(requestConfig)
        .build();
    client.start();
  }

  private MethodMetadata methodMetadata;

  public ReactiveHttpAsyncClient(MethodMetadata methodMetadata) {
    this.methodMetadata = methodMetadata;
  }

  @Override
  public Mono<Response> executeRequest(Mono<Request> mono) {
    return mono.flatMap(request -> {
      final HttpUriRequest httpUriRequest;
      if (request.method().equalsIgnoreCase("get")) {
        httpUriRequest = new HttpGet(request.url());
      } else if (request.method().equalsIgnoreCase("post")) {
        HttpPost httpPost = new HttpPost(request.url());
        httpPost.setEntity(new ByteArrayEntity(request.body()));
        httpUriRequest = httpPost;
      } else if (request.method().equalsIgnoreCase("put")) {
        HttpPut httpPut = new HttpPut(request.url());
        httpPut.setEntity(new ByteArrayEntity(request.body()));
        httpUriRequest = httpPut;
      } else if (request.method().equalsIgnoreCase("delete")) {
        httpUriRequest = new HttpDelete(request.url());
      } else if (request.method().equalsIgnoreCase("option")) {
        httpUriRequest = new HttpOptions(request.url());
      } else if (request.method().equalsIgnoreCase("patch")) {
        httpUriRequest = new HttpPatch(request.url());
      } else if (request.method().equalsIgnoreCase("head")) {
        httpUriRequest = new HttpHead(request.url());
      } else {
        throw new RuntimeException("not support method: " + request.method());
      }
      if (request.headers() != null && request.headers().size() > 0) {
        Header[] headers = request.headers().entrySet().stream()
            .filter(entry -> !entry.getKey().equalsIgnoreCase("content-length"))
            .map(entry -> {
              return new BasicHeader(entry.getKey(), entry.getValue().stream().findFirst().get());
            })
            .toArray(Header[]::new);
        httpUriRequest.setHeaders(headers);
      }
      return Mono.defer(() -> {
        return Mono.create(monoSink -> {
          client.execute(httpUriRequest, new FutureCallback<HttpResponse>() {
            @Override
            public void completed(HttpResponse httpResponse) {
              try {
                Response.Builder responseBuilder = Response.builder();
                Map<String, Collection<String>> headerMap = Stream.of(httpResponse.getAllHeaders())
                        .collect(Collectors.toMap(header -> header.getName(), header -> Arrays.asList(new String[]{header.getValue()})));
                responseBuilder.request(request).status(httpResponse.getStatusLine().getStatusCode())
                    .headers(headerMap);

                if (httpResponse.getEntity().getContentLength() == 0) {
                  responseBuilder.body(new byte[0]);
                } else {
                  responseBuilder.body(EntityUtils.toByteArray(httpResponse.getEntity()));
                }
                monoSink.success(responseBuilder.build());
              } catch (IOException e) {
                monoSink.error(e);
              }
            }

            @Override
            public void failed(Exception e) {
              monoSink.error(e);
            }

            @Override
            public void cancelled() {
            }
          });
        });
      });
    });
  }
}
