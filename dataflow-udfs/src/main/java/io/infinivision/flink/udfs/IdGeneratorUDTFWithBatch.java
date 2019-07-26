package io.infinivision.flink.udfs;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.google.common.collect.Lists;
import okhttp3.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.table.api.functions.FunctionContext;
import org.apache.flink.table.api.functions.TableFunction;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

public class IdGeneratorUDTFWithBatch extends TableFunction<BaseRow> implements ListCheckpointed<IdDTO> {

    private static final Logger LOG = LoggerFactory.getLogger(IdGeneratorUDTFWithBatch.class);

    private static Random RANDOM = new Random();

    private OkHttpClient okHttpClient;
    private List<String> urlList = Lists.newArrayList();
    private IdDTO currentId = new IdDTO();

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);

        //read request url
        String urls = context.getJobParameter("id.generator.url", "");
        if (StringUtils.isBlank(urls)) {
            throw new Exception("request url is empty!!");
        }
        //set request url
        Stream.of(StringUtils.split(urls, ",")).forEach(url -> urlList.add(url));

        //init http client
        Dispatcher dispatcher = new Dispatcher();
        dispatcher.setMaxRequestsPerHost(50);
        dispatcher.setMaxRequests(50);
        this.okHttpClient = new OkHttpClient.Builder().dispatcher(dispatcher).build();
    }

    public void eval(Object... values) throws Exception {

        while (true) {
            if ((currentId.getBegin() != 0 && currentId.getEnd() != 0) && (currentId.getBegin() <= currentId.getEnd())) {
                GenericRow reuseRow = new GenericRow(1);
                reuseRow.update(0, currentId.getBeginAndNext());
                collect(GenericRow.copyReference(reuseRow));
                return;
            }
            requestBatchIds(currentId);
        }
    }

    @Override
    public DataType getResultType(Object[] arguments, Class[] argTypes) {
        return DataTypes.createRowType(DataTypes.LONG);
    }

    @Override
    public List<IdDTO> snapshotState(long checkpointId, long timestamp) throws Exception {
        System.out.println("snapshotState @:" + new Date().getTime() + "||" + checkpointId + "||" + timestamp + " with:" + currentId.toString());
        return Collections.singletonList(currentId);
    }

    @Override
    public void restoreState(List<IdDTO> state) throws Exception {
        for (IdDTO dto : state) {
            System.out.println("restoreState @:" + new Date().getTime() + " with:" + dto.toString());
            currentId.init(dto.getBegin(), dto.getEnd());
        }
    }

    public void requestBatchIds(IdDTO next) throws IOException {

        // 表单键值对
        RequestBody formBody = new FormBody.Builder()
                .build();

        // 请求
        Request request = new Request.Builder()
                .url(urlList.get(RANDOM.nextInt(urlList.size())))
                .post(formBody)
                .build();

        try (Response response = okHttpClient.newCall(request).execute()) {
            if (!response.isSuccessful()) throw new IOException("Unexpected code " + response);

            String data = response.body().string();
            JSONObject jsonObject = JSON.parseObject(data);
            int code = jsonObject.getIntValue("code");

            if (code == 0) {
                Long[] value = jsonObject.getObject("value", new TypeReference<Long[]>() {
                });
                next.init(value[0], value[1]);
            } else {
                throw new RuntimeException("IdGeneratorUDTFWithBatch" + jsonObject.getString("error"));
            }

        }
    }
}
