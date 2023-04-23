package org.example.table;

import com.google.gson.reflect.TypeToken;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.Watch;
import okhttp3.OkHttpClient;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import java.util.concurrent.TimeUnit;

public class PodSourceFunction extends RichSourceFunction<RowData> implements ResultTypeQueryable<RowData> {
    private final TypeInformation<RowData> producedType;
    private Watch<V1Pod> currentWatch;

    public PodSourceFunction(TypeInformation<RowData> producedType) {
        this.producedType = producedType;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedType;
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        ApiClient client = Config.defaultClient();
        // infinite timeout
        OkHttpClient httpClient =
                client.getHttpClient().newBuilder().readTimeout(0, TimeUnit.SECONDS).build();
        client.setHttpClient(httpClient);
        Configuration.setDefaultApiClient(client);

        CoreV1Api api = new CoreV1Api();

        Watch<V1Pod> watch =
                Watch.createWatch(
                        client,
                        api.listPodForAllNamespacesCall(
                                null, null, null, null, null, null, null, null, null, Boolean.TRUE, null),
                        new TypeToken<Watch.Response<V1Pod>>() {
                        }.getType());
        currentWatch = watch;

        try {
            for (Watch.Response<V1Pod> item : watch) {
                ctx.collect(convert(item));
            }
        } finally {
            watch.close();
        }
    }

    private RowData convert(Watch.Response<V1Pod> item) {
        GenericRowData row = new GenericRowData(3);
        switch (item.type) {
            case "ADDED":
                row.setRowKind(RowKind.INSERT);
                break;
            case "DELETED":
                row.setRowKind(RowKind.DELETE);
                break;
            default:
                row.setRowKind(RowKind.UPDATE_AFTER);
        }
        row.setField(0, item.object.getMetadata().getNamespace());
        row.setField(1, item.object.getMetadata().getName());
        row.setField(2, item.object.getStatus().getPhase());
        return row;
    }

    @Override
    public void cancel() {
        try {
            currentWatch.close();
        } catch (Throwable t) {
            // ignore
        }
    }
}