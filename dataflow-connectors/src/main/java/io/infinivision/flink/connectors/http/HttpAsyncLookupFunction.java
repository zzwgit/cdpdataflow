package io.infinivision.flink.connectors.http;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.google.common.collect.Lists;
import okhttp3.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.api.functions.AsyncTableFunction;
import org.apache.flink.table.api.functions.FunctionContext;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.RowType;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.util.TableProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.SocketFactory;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

public class HttpAsyncLookupFunction extends AsyncTableFunction<BaseRow> {
    private static final Logger LOG = LoggerFactory.getLogger(HttpAsyncLookupFunction.class);

    private static Random random = new Random();

    private List<String> URL_LIST = Lists.newArrayList();

    private final TableProperties tableProperties;
    private final RowType returnType;
    private OkHttpClient okHttpClient;

    public HttpAsyncLookupFunction(TableProperties tableProperties,
                                   RowType returnType) {
        this.tableProperties = tableProperties;
        this.returnType = returnType;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        String urls = tableProperties.getString(HttpTableOptions.REQUEST_URL);
        if (StringUtils.isBlank(urls)) {
            throw new Exception("HttpAsyncLookupFunction request url is empty!");
        }
        Stream.of(StringUtils.split(urls, ",")).forEach(url -> URL_LIST.add(url));

        Dispatcher dispatcher = new Dispatcher();
        dispatcher.setMaxRequestsPerHost(100);
        dispatcher.setMaxRequests(100);
        this.okHttpClient = new OkHttpClient.Builder()
                .socketFactory(new SocketFactory() {
                    private SocketFactory delegate = SocketFactory.getDefault();

                    @Override
                    public Socket createSocket() throws IOException {
                        Socket value = delegate.createSocket();
                        value.setReuseAddress(true);
                        return value;
                    }

                    @Override
                    public Socket createSocket(String s, int i) throws IOException, UnknownHostException {
                        Socket value = delegate.createSocket(s, i);
                        value.setReuseAddress(true);
                        return value;
                    }

                    @Override
                    public Socket createSocket(String s, int i, InetAddress inetAddress, int i1) throws IOException, UnknownHostException {
                        Socket value = delegate.createSocket(s, i, inetAddress, i1);
                        value.setReuseAddress(true);
                        return value;
                    }

                    @Override
                    public Socket createSocket(InetAddress inetAddress, int i) throws IOException {
                        Socket value = delegate.createSocket(inetAddress, i);
                        value.setReuseAddress(true);
                        return value;
                    }

                    @Override
                    public Socket createSocket(InetAddress inetAddress, int i, InetAddress inetAddress1, int i1) throws IOException {
                        Socket value = delegate.createSocket(inetAddress, i, inetAddress1, i1);
                        value.setReuseAddress(true);
                        return value;
                    }
                })
                .dispatcher(dispatcher).build();

    }

    public void eval(final ResultFuture<BaseRow> resultFuture, Object inputs) throws Exception {
        if (URL_LIST.size() == 0) {
            throw new Exception("HttpAsyncLookupFunction request url is empty!");
        }

        // 表单键值对
        RequestBody formBody = new FormBody.Builder()
                .build();

        // 请求
        Request request = new Request.Builder()
                .url(URL_LIST.get(random.nextInt(URL_LIST.size())))
                .post(formBody)
                .build();

        okHttpClient.newCall(request).enqueue(new Callback() { // 回调

            public void onResponse(Call call, Response response) throws IOException {

                try {
                    if (200 == response.code()) {
                        String data = response.body().string();
                        response.close();

                        JSONObject jsonObject = JSON.parseObject(data);
                        int code = jsonObject.getIntValue("code");

                        List<BaseRow> results = new ArrayList<>();
                        if (code == 0) {
                            Long[] value = jsonObject.getObject("value", new TypeReference<Long[]>() {
                            });
                            GenericRow reuseRow = new GenericRow(returnType.getArity());
                            reuseRow.update(0, inputs);
                            reuseRow.update(1, value[0]);
                            results.add(GenericRow.copyReference(reuseRow));
                            resultFuture.complete(results);
                        } else {
                            resultFuture.complete(Collections.emptyList());
                            throw new RuntimeException("HttpAsyncLookupFunction" + jsonObject.getString("error"));
                        }
                    }
                } catch (Exception e) {
                    resultFuture.complete(Collections.emptyList());
                    LOG.error("HttpAsyncLookupFunction onResponse failed:" + e);
                }

            }

            public void onFailure(Call call, IOException e) {
                // 请求失败调用
                System.out.println("-------------------------------------------------------------------HttpAsyncLookupFunction onFailure:" + e.getMessage());
                e.printStackTrace();
                resultFuture.complete(Collections.emptyList());
            }
        });

    }

    @Override
    public DataType getResultType(Object[] arguments, Class[] argTypes) {
        return this.returnType;
    }

    @Override
    public void close() throws Exception {
        LOG.info("Close HttpAsyncLookupFunction");
        super.close();
    }

}
