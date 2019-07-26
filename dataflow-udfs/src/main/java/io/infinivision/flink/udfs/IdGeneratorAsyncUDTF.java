package io.infinivision.flink.udfs;

import com.google.common.collect.Lists;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.async.Callback;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.api.functions.AsyncTableFunction;
import org.apache.flink.table.api.functions.FunctionContext;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.types.Row;
import org.apache.http.HttpStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.stream.Stream;

public class IdGeneratorAsyncUDTF extends AsyncTableFunction<Row> {

    private static Random random = new Random();

    private List<String> URL_LIST = Lists.newArrayList();

    /**
     * Setup method for user-defined function. It can be used for initialization work.
     *
     * <p>By default, this method does nothing.
     *
     * @param context
     */
    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);

        String urls = context.getJobParameter("id.generator.url", "");
        if (StringUtils.isBlank(urls)) {
            throw new Exception("request url is empty!");
        }
        Stream.of(StringUtils.split(urls, ",")).forEach(url -> URL_LIST.add(url));
    }

    public void eval(ResultFuture<Row> resultFuture, String input) throws Exception {

        if (URL_LIST.size() == 0) {
            throw new Exception("request url is empty!");
        }

        Future<HttpResponse<JsonNode>> future = Unirest.post(URL_LIST.get(random.nextInt(URL_LIST.size())))
                .asJsonAsync(new Callback<JsonNode>() {

                    public void failed(UnirestException e) {

                    }

                    public void completed(HttpResponse<JsonNode> response) {

                        if (HttpStatus.SC_OK == response.getStatus()) {
                            JsonNode body = response.getBody();
                            Integer code = (Integer) body.getObject().get("code");

                            List<Row> results = new ArrayList<>();
                            if (code == 0) {
                                Integer value = (Integer) body.getObject().get("value");
                                Row row = new Row(1);
                                row.setField(0, value);
                                results.add(row);
                                resultFuture.complete(results);
                                return;
                            } else {
                                throw new RuntimeException(String.valueOf(body.getObject().get("error")));
                            }
                        }
                    }

                    public void cancelled() {
                        System.out.println("The IdGeneratorAsyncUDTF request has been cancelled");
                    }

                });

    }

    @Override
    public DataType getResultType(Object[] arguments, Class[] argTypes) {
        return DataTypes.createRowType(DataTypes.INT);
    }
}
