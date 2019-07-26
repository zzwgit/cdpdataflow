package io.infinivision.flink.udfs;

import com.google.common.collect.Lists;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.api.functions.FunctionContext;
import org.apache.flink.table.api.functions.ScalarFunction;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

public class IdGeneratorUDF extends ScalarFunction {

    private static final Logger LOG = LoggerFactory.getLogger(IdGeneratorUDF.class);

    private static Random random = new Random();
    private List<String> URL_LIST = Lists.newArrayList();

    private static int index = 0;

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

    public Integer eval(String str) throws Exception {

        //str = "http://172.19.0.108:8081/v1/id/next";
        if (URL_LIST.size() == 0) {
            throw new Exception("request url is empty!");
        }
        int retry = 1;
        while (retry <= 3) {

            try {
                HttpResponse<JsonNode> response = Unirest.post(URL_LIST.get(random.nextInt(URL_LIST.size()))).asJson();

                if (HttpStatus.SC_OK == response.getStatus()) {
                    JsonNode body = response.getBody();
                    Integer code = (Integer) body.getObject().get("code");

                    if (code == 0) {
                        Integer value = (Integer) body.getObject().get("value");
                        return value;
                    } else {
                        LOG.warn("IdGeneratorUDF failed after request:", String.valueOf(body.getObject().get("error")));
                    }
                } else {
                    LOG.error("IdGeneratorUDF response status is {},{} :", response.getStatus(), response.getBody().toString());
                }

            } catch (Exception e) {
                LOG.error("IdGeneratorUDF failed:", e);
            }

            retry++;
            Thread.sleep(500);
        }
        return null;

    }

}
