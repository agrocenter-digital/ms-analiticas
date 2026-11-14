package com.agrocenter.analiticas.kinesis;

import com.agrocenter.analiticas.eventos.AnaliticasProcessor;
import com.agrocenter.analiticas.eventos.CuarentenaService;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.retrieval.polling.PollingConfig;

@Component
@ConditionalOnProperty(name = "agrocenter.analiticas.kinesis.enabled", havingValue = "true")
public class AnaliticasKinesisConsumers implements HealthIndicator {
    private static final Logger log = LoggerFactory.getLogger(AnaliticasKinesisConsumers.class);
    private final AnaliticasProcessor processor;
    private final CuarentenaService cuarentena;
    private final EstadoConsumidores estados = new EstadoConsumidores();
    private final AtomicBoolean stopping = new AtomicBoolean();
    private final List<Scheduler> schedulers = new ArrayList<>();
    @Value("${aws.region:us-east-1}") private String regionName;
    @Value("${agrocenter.analiticas.kinesis.business-stream}") private String businessStream;
    @Value("${agrocenter.analiticas.kinesis.stock-stream}") private String stockStream;
    @Value("${agrocenter.analiticas.kinesis.business-application}") private String businessApplication;
    @Value("${agrocenter.analiticas.kinesis.stock-application}") private String stockApplication;
    @Value("${KINESIS_INITIAL_POSITION:TRIM_HORIZON}") private String initialPosition;
    @Value("${agrocenter.analiticas.kinesis.retry-delay-ms:5000}") private long retryDelayMs;
    @Value("${agrocenter.analiticas.kinesis.max-lag-ms:3600000}") private long maxLagMs;
    @Value("${agrocenter.analiticas.kinesis.max-silence-ms:120000}") private long maxSilenceMs;
    private KinesisAsyncClient kinesis;
    private DynamoDbAsyncClient dynamo;
    private CloudWatchAsyncClient cloudWatch;

    public AnaliticasKinesisConsumers(AnaliticasProcessor processor, CuarentenaService cuarentena) { this.processor = processor; this.cuarentena = cuarentena; }
    @Override public Health health() { return stopping.get() ? Health.down().withDetail("kinesis", "cerrando").build() : estados.salud(maxLagMs, maxSilenceMs); }

    @PostConstruct public void start() {
        if (businessApplication.equals(stockApplication)) throw new IllegalStateException("Cada stream requiere una aplicacion KCL distinta");
        validarPosicionInicial(initialPosition);
        Region region = Region.of(regionName);
        kinesis = KinesisAsyncClient.builder().region(region).build();
        dynamo = DynamoDbAsyncClient.builder().region(region).build();
        cloudWatch = CloudWatchAsyncClient.builder().region(region).build();
        iniciar(businessStream, businessApplication);
        iniciar(stockStream, stockApplication);
    }

    private void iniciar(String stream, String application) {
        EstadoConsumidores.Stream estado = estados.registrar(application);
        Scheduler[] referencia = new Scheduler[1];
        ConfigsBuilder c = new ConfigsBuilder(stream, application, kinesis, dynamo, cloudWatch,
                UUID.randomUUID().toString(), () -> new AnaliticasShardProcessor(stream, processor, cuarentena, estado,
                        stopping::get, () -> referencia[0].startGracefulShutdown(), retryDelayMs));
        Scheduler scheduler = new Scheduler(c.checkpointConfig(), c.coordinatorConfig(), c.leaseManagementConfig(),
                c.lifecycleConfig(), c.metricsConfig().metricsLevel(MetricsLevel.NONE), c.processorConfig().callProcessRecordsEvenForEmptyRecordList(true),
                c.retrievalConfig().retrievalSpecificConfig(new PollingConfig(stream, kinesis))
                        .initialPositionInStreamExtended(InitialPositionInStreamExtended.newInitialPosition(validarPosicionInicial(initialPosition))));
        referencia[0] = scheduler;
        schedulers.add(scheduler);
        estado.worker = new Thread(() -> { try { scheduler.run(); } finally { estado.fatal.set(true); } }, "kcl-analiticas-" + application);
        estado.worker.start();
    }

    static InitialPositionInStream validarPosicionInicial(String valor) {
        if ("TRIM_HORIZON".equals(valor)) return InitialPositionInStream.TRIM_HORIZON;
        if ("LATEST".equals(valor)) return InitialPositionInStream.LATEST;
        throw new IllegalArgumentException("KINESIS_INITIAL_POSITION debe ser TRIM_HORIZON o LATEST");
    }

    @PreDestroy public void stop() {
        stopping.set(true);
        for (Scheduler scheduler : schedulers) {
            try { scheduler.startGracefulShutdown().get(20, TimeUnit.SECONDS); }
            catch (Exception e) { log.error("No se pudo detener un consumidor de analiticas", e); }
        }
        if (kinesis != null) kinesis.close();
        if (dynamo != null) dynamo.close();
        if (cloudWatch != null) cloudWatch.close();
    }
}
