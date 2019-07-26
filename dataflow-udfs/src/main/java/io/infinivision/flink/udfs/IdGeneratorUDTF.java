package io.infinivision.flink.udfs;

import com.google.common.collect.Lists;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.api.functions.TableFunction;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.types.Row;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

public class IdGeneratorUDTF extends TableFunction<Row> {

    private static final Logger LOG = LoggerFactory.getLogger(IdGeneratorUDTF.class);

    private static Random random = new Random();

    private List<String> URL_LIST = Lists.newArrayList();

    public void eval(String str) throws Exception {

        if (StringUtils.isBlank(str)) {
            throw new Exception("request url is empty!");
        }
        if (URL_LIST.size() == 0) {
            Stream.of(StringUtils.split(str, ",")).forEach(url -> URL_LIST.add(url));
        }
        int retry = 1;
        while (true) {
            HttpResponse<JsonNode> response = Unirest.post(URL_LIST.get(random.nextInt(URL_LIST.size()))).asJson();
            if (HttpStatus.SC_OK == response.getStatus()) {
                JsonNode body = response.getBody();
                Integer code = (Integer) body.getObject().get("code");

                if (code == 0) {
                    Integer value = (Integer) body.getObject().get("value");
                    Row row = new Row(1);
                    row.setField(0, value);
                    collect(row);
                    break;
                } else {
                    if (retry == 3) {
                        throw new RuntimeException(String.valueOf(body.getObject().get("error")));
                    }
                }
            }

            retry++;
            Thread.sleep(500);
        }

    }

    @Override
    public DataType getResultType(Object[] arguments, Class[] argTypes) {
        return DataTypes.createRowType(DataTypes.INT);
    }

}
