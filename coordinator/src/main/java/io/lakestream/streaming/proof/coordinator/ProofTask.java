/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package io.lakestream.streaming.proof.coordinator;

import io.lakestream.streaming.proof.common.LongSeq;
import io.lakestream.streaming.proof.common.ProofDriver;
import io.lakestream.streaming.proof.common.WebhookNotificationService;
import io.lakestream.streaming.proof.common.WorkerHttpClient;
import io.lakestream.streaming.proof.common.records.Checkpoints;
import io.lakestream.streaming.proof.common.records.Configs;
import io.lakestream.streaming.proof.common.records.ConsumerCheckPoint;
import io.lakestream.streaming.proof.common.records.Driver;
import io.lakestream.streaming.proof.common.records.Drivers;
import io.lakestream.streaming.proof.common.records.ErrorOccurrence;
import io.lakestream.streaming.proof.common.records.LatencyMetricSnapshot;
import io.lakestream.streaming.proof.common.records.LatencySummary;
import io.lakestream.streaming.proof.common.records.NewConsumers;
import io.lakestream.streaming.proof.common.records.NewProducers;
import io.lakestream.streaming.proof.common.records.ProducerCheckpoint;
import io.lakestream.streaming.proof.common.records.Proof;
import io.lakestream.streaming.proof.common.records.ProofCheckpointSummary;
import io.lakestream.streaming.proof.common.records.ProofClusterTarget;
import io.lakestream.streaming.proof.common.records.ProofDetails;
import io.lakestream.streaming.proof.common.records.ProofPerformanceSummary;
import io.lakestream.streaming.proof.common.records.ProofReport;
import io.lakestream.streaming.proof.common.records.ProofSummary;
import io.lakestream.streaming.proof.common.records.ProofTimeSeriesPoint;
import io.lakestream.streaming.proof.common.records.ProofWorkerMetricsSnapshot;
import io.lakestream.streaming.proof.worker.DriverCache;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.asynchttpclient.Dsl;

/**
 * Manages a single streaming proof verification task across distributed workers.
 *
 * <p>ProofTask is the operational core of the verification framework, responsible for:
 * <ul>
 *   <li>Initializing and managing the messaging system driver</li>
 *   <li>Creating and configuring topics with appropriate partitioning</li>
 *   <li>Distributing producers and consumers across worker nodes</li>
 *   <li>Collecting and aggregating checkpoints from all workers</li>
 *   <li>Verifying message delivery guarantees through checkpoint analysis</li>
 *   <li>Detecting violations like message loss, duplication, or reordering</li>
 *   <li>Providing detailed diagnostics for any detected issues</li>
 * </ul>
 *
 * <p>The verification process works by:
 * <ul>
 *   <li>Producers send sequentially numbered messages with unique keys</li>
 *   <li>Consumers track received sequences using range-based checkpoints</li>
 *   <li>The coordinator periodically collects and aggregates these checkpoints</li>
 *   <li>Aggregated checkpoints are analyzed to detect guarantee violations</li>
 *   <li>Results are summarized with detailed diagnostics for any issues</li>
 * </ul>
 *
 *
 * @see io.lakestream.streaming.proof.common.records.Proof
 * @see io.lakestream.streaming.proof.common.records.ConsumerCheckPoint
 * @see io.lakestream.streaming.proof.common.records.ProducerCheckpoint
 */
@Slf4j
@Getter
public class ProofTask {

    private static final String REPORT_SETTING_FINAL_WAIT_SECONDS = "finalWaitSeconds";
    private static final String REPORT_SETTING_FINAL_VERIFICATION_TIMEOUT_SECONDS =
            "finalVerificationTimeoutSeconds";
    private static final int DEFAULT_FINAL_WAIT_SECONDS = 30;

    /** The proof test configuration */
    private final Proof proof;

    /** Executor for scheduling periodic checkpoint verification */
    private final ScheduledExecutorService executor;

    /** System-wide configuration for workers and drivers */
    private final Configs configs;

    /** The messaging system driver implementation */
    private final ProofDriver proofDriver;

    /** Driver name used for admin operations in the primary proof task. */
    private final String adminDriverName;

    /** Lazily initialized topic admin drivers for non-primary geo roles. */
    private final DriverCache topicDriverCache;

    /** Optional resolver used by tests to inject mocked topic admin drivers. */
    private final Function<String, ProofDriver> topicDriverResolver;

    /** List of HTTP clients for communicating with worker nodes */
    private final List<WorkerHttpClient> clients;
    
    /** Service for sending webhook notifications */
    private final WebhookNotificationService webhookService;

    /** Whether this proof uses Pulsar Shared subscription mode */
    private final boolean sharedMode;

    /** High watermarks per key for shared-mode verification */
    private Map<String, Long> highWatermarks = new HashMap<>();

    /** Current checkpoint being verified */
    private ProducerCheckpoint inCheck = new ProducerCheckpoint();

    /** Last successfully verified producer checkpoint */
    private ProducerCheckpoint lastVerifiedProducerCheckpoint = new ProducerCheckpoint();

    /** Last successfully verified consumer checkpoint */
    private ConsumerCheckPoint lastVerifiedConsumerCheckpoint = new ConsumerCheckPoint();

    /** Latest producer checkpoint received */
    private ProducerCheckpoint latestProducerCheckpoint = new ProducerCheckpoint();

    /** Latest consumer checkpoint received */
    @Setter(AccessLevel.PACKAGE)
    private ConsumerCheckPoint latestConsumerCheckpoint = new ConsumerCheckPoint();

    /** Last failed producer checkpoint */
    private ProducerCheckpoint lastFailedProducerCheckpoint = new ProducerCheckpoint();

    /** Last failed consumer checkpoint */
    private ConsumerCheckPoint lastFailedConsumerCheckpoint = new ConsumerCheckPoint();

    /** Number of checkpoint verification timeouts */
    private int timeouts;

    /** Last observed verified count, used to detect when verification progress stalls. */
    private long lastVerifiedObserved;

    /** Wall-clock time (millis) when the verified count last increased. */
    private long lastVerifiedChangeMillis = System.currentTimeMillis();

    /** Wall-clock time (millis) the run completed; freezes the stall value. 0 while running. */
    private long completionMillis;

    /** Peak verification stall observed during the run, in millis. */
    private long maxVerifiedStallMillis;

    /** Timestamp of when the current checkpoint verification started */
    private long checkPointInCheckTimeStamps;

    private boolean failed = false;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean stopping = new AtomicBoolean(false);
    private final AtomicBoolean manuallyStopped = new AtomicBoolean(false);
    private final AtomicBoolean completionHandled = new AtomicBoolean(false);
    private final List<ProofTimeSeriesPoint> timeSeries = new ArrayList<>();
    private volatile ProofPerformanceSummary latestPerformanceSummary;
    private volatile List<ProofClusterTarget> clusterTargetsSnapshot;
    private volatile String startFailureReason;

    // --- Windowed rate tracking ---
    private long prevPublishedMessages;
    private long prevConsumedMessages;
    private long prevPublishErrors;
    private long prevPublishedBytes;
    private long prevConsumedBytes;
    private long prevElapsedSeconds;

    public boolean isRunning() {
        return running.get();
    }

    /**
     * Creates a new proof task with the specified configuration.
     *
     * @param proof The proof test configuration
     * @param configs System-wide configuration for workers and drivers
     */
    public ProofTask(Proof proof, Configs configs, ProofDriver driver) {
        this(proof, configs, driver, null);
    }

    ProofTask(
            Proof proof,
            Configs configs,
            ProofDriver driver,
            Function<String, ProofDriver> topicDriverResolver) {
        this.proof = proof;
        this.executor = Executors.newSingleThreadScheduledExecutor();
        this.configs = configs;
        this.clients = new ArrayList<>(configs.workers().size());
        this.proofDriver = driver;
        this.adminDriverName = resolveAdminDriverName(proof);
        this.topicDriverCache = new DriverCache();
        this.topicDriverResolver = topicDriverResolver;
        this.webhookService = new WebhookNotificationService();
        this.sharedMode = proof.getPulsar() != null
                && proof.getPulsar().isSharedMode();
        applyTopicDefaults();
        configs.workers().forEach((k, v) -> {
            WorkerHttpClient client = new WorkerHttpClient(Dsl.asyncHttpClient(), v);
            clients.add(client);
        });
    }

    /**
     * Starts the proof test execution.
     * This method:
     * <ul>
     *   <li>Initializes the messaging system driver</li>
     *   <li>Creates the required topic</li>
     *   <li>Starts consumers and producers across workers</li>
     *   <li>Begins periodic checkpoint verification</li>
     * </ul>
     */
    public void start() {
        running.set(true);
        startFailureReason = null;
        lastVerifiedObserved = 0;
        lastVerifiedChangeMillis = System.currentTimeMillis();
        completionMillis = 0;
        maxVerifiedStallMillis = 0;
        try {
            createProofTopics();
            startConsumers();
            startProducers();
            String formattedTimestamp = DateTimeFormatter.ISO_LOCAL_DATE_TIME
                    .format(LocalDateTime.now());
            proof.setStartTime(formattedTimestamp);
            scheduleCheckpoint();
            log.info("Started the proof {}", proof);
        } catch (Exception e) {
            handleStartFailure(e);
            throw e;
        }
    }

    private void handleStartFailure(Exception error) {
        startFailureReason = rootCauseMessage(error);
        // Ensure a start time is recorded even when startup fails before
        // setStartTime() is reached, so failed runs still group/sort correctly.
        if (proof.getStartTime() == null || proof.getStartTime().isBlank()) {
            proof.setStartTime(DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(LocalDateTime.now()));
        }
        freezeClusterTargetsSnapshot();
        cleanupPartiallyStartedWorkers();
        closeTopicDrivers();
        latestPerformanceSummary = buildPerformanceSummary(getSummary());
        completionMillis = System.currentTimeMillis();
        running.set(false);
        stopping.set(false);
        log.warn("Proof {} failed to start: {}", proof.getId(), startFailureReason, error);
    }

    void createProofTopics() {
        createTopicOnDrivers(proof.getTopic(), resolveOutputTopicDriverNames());

        if (proof.getFeatures().contains("exactly_once")) {
            createTopicOnDrivers(proof.getTopic() + "_transactional", resolveTransactionalTopicDriverNames());
        }
    }

    void deleteProofTopics() {
        if (proof.getFeatures() != null && proof.getFeatures().contains("exactly_once")) {
            deleteTopicOnDrivers(proof.getTopic() + "_transactional", resolveTransactionalTopicDriverNames());
        }
        deleteTopicOnDrivers(proof.getTopic(), resolveOutputTopicDriverNames());
    }

    private void createTopicOnDrivers(String topicName, List<String> driverNames) {
        Map<String, String> topicConfig = proof.getKafka() != null
                ? proof.getKafka().getTopicConfig() : null;
        runTopicOperation(driverNames, driver ->
                driver.createTopic(topicName, topicPartitions(), topicConfig));
    }

    /**
     * Fills in the topic settings the proof leaves unset. A Pulsar scalable topic is also given its
     * topic:// name, which is what makes the Pulsar driver use a scalable topic.
     */
    private void applyTopicDefaults() {
        if (!proof.isScalableTopic()) {
            if (proof.getPartitions() == null) {
                proof.setPartitions(Proof.DEFAULT_PARTITIONS);
            }
            return;
        }
        if (!proof.getTopic().contains("://")) {
            proof.setTopic("topic://public/default/" + proof.getTopic());
        }
        if (proof.getPulsar().getInitialSegments() == null) {
            proof.getPulsar().setInitialSegments(Proof.DEFAULT_PARTITIONS);
        }
    }

    /** Partitions of a classic topic, or initial segments of a Pulsar scalable topic */
    private int topicPartitions() {
        return proof.isScalableTopic() ? proof.getPulsar().getInitialSegments() : proof.getPartitions();
    }

    private void deleteTopicOnDrivers(String topicName, List<String> driverNames) {
        for (String driverName : driverNames) {
            try {
                resolveTopicDriver(driverName).deleteTopic(topicName);
            } catch (Exception e) {
                log.warn("Failed to delete topic {} on driver {} for proof {}",
                        topicName, driverName, proof.getId(), e);
            }
        }
    }

    private void runTopicOperation(List<String> driverNames, TopicDriverOperation operation) {
        for (String driverName : driverNames) {
            try {
                operation.run(resolveTopicDriver(driverName));
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private ProofDriver resolveTopicDriver(String driverName) {
        if (topicDriverResolver != null) {
            ProofDriver resolved = topicDriverResolver.apply(driverName);
            if (resolved == null) {
                throw new IllegalArgumentException("Driver resolver returned null for " + driverName);
            }
            return resolved;
        }

        if (driverName == null || driverName.equals(adminDriverName)) {
            return proofDriver;
        }

        Driver driverConfig = configs.drivers().get(driverName);
        if (driverConfig == null) {
            throw new IllegalArgumentException("Driver " + driverName + " not found. "
                    + "Available drivers: " + configs.drivers().keySet());
        }
        return topicDriverCache.getDriver(driverName, driverConfig);
    }

    private List<String> resolveOutputTopicDriverNames() {
        Drivers drivers = proof.getDrivers();
        if (drivers == null) {
            return Collections.singletonList(adminDriverName);
        }

        LinkedHashSet<String> driverNames = new LinkedHashSet<>();
        addDriverName(driverNames, drivers.admin());
        addDriverName(driverNames, drivers.producer());
        addDriverName(driverNames, drivers.consumer());
        if (driverNames.isEmpty()) {
            driverNames.add(adminDriverName);
        }
        return List.copyOf(driverNames);
    }

    private List<String> resolveTransactionalTopicDriverNames() {
        Drivers drivers = proof.getDrivers();
        if (drivers == null) {
            return Collections.singletonList(adminDriverName);
        }

        LinkedHashSet<String> driverNames = new LinkedHashSet<>();
        addDriverName(driverNames, drivers.admin());
        addDriverName(driverNames, drivers.producer());
        if (driverNames.isEmpty()) {
            driverNames.add(adminDriverName);
        }
        return List.copyOf(driverNames);
    }

    private static void addDriverName(Set<String> driverNames, String driverName) {
        if (driverName != null && !driverName.isBlank()) {
            driverNames.add(driverName);
        }
    }

    private static String resolveAdminDriverName(Proof proof) {
        Drivers drivers = proof.getDrivers();
        if (drivers != null && drivers.admin() != null && !drivers.admin().isBlank()) {
            return drivers.admin();
        }
        return proof.getDriver();
    }

    private void closeTopicDrivers() {
        try {
            topicDriverCache.close();
        } catch (RuntimeException e) {
            log.warn("Failed to close topic admin drivers for proof {}", proof.getId(), e);
        }
    }

    @FunctionalInterface
    private interface TopicDriverOperation {
        void run(ProofDriver driver) throws Exception;
    }

    private void cleanupPartiallyStartedWorkers() {
        clients.forEach(client -> {
            try {
                client.stopProducers(proof.getId()).join();
            } catch (Exception e) {
                log.debug("Ignoring producer cleanup failure for proof {}", proof.getId(), e);
            }
            try {
                client.stopAndRemoveConsumers(proof.getId()).join();
            } catch (Exception e) {
                log.debug("Ignoring consumer cleanup failure for proof {}", proof.getId(), e);
            }
        });
    }

    private static String rootCauseMessage(Throwable error) {
        Throwable current = error;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        if (current.getMessage() != null && !current.getMessage().isBlank()) {
            return current.getMessage();
        }
        return current.getClass().getSimpleName();
    }

    /**
     * Distributes consumers across available worker nodes.
     * Attempts to evenly distribute the configured number of consumers
     * across all available workers.
     */
    private void startConsumers() {
        int workerCount = clients.size();
        // Distribute consumers to workers
        int totalConsumers = proof.getConsumers();
        int baseConsumersPerWorker = totalConsumers / workerCount;
        int consumerRemainder = totalConsumers % workerCount;
        String driverName = proof.getDrivers() == null ? proof.getDriver() : proof.getDrivers().consumer();
        Map<String, Object> pulsarConsumerConfig = proof.getPulsar() != null
                ? proof.getPulsar().getConsumerConfig() : null;
        String pulsarConsumerType = proof.getPulsar() != null ? proof.getPulsar().getConsumerType() : null;
        for (int i = 0; i < clients.size(); i++) {
            int consumerCount = baseConsumersPerWorker + (i == workerCount - 1 ? consumerRemainder : 0);
            NewConsumers record = new NewConsumers(
                    proof.getId(),
                    proof.getTopic(),
                    topicPartitions(),
                    consumerCount,
                    TimeUnit.SECONDS.toMillis(proof.getConsumeDelay()),
                    proof.getMessageSize(),
                    driverName,
                    configs.drivers().get(driverName),
                    pulsarConsumerConfig,
                    pulsarConsumerType
            );
            try {
                clients.get(i).startConsumers(record).join();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Distributes producers across available worker nodes.
     * Attempts to evenly distribute producers, keys, and message rates
     * across all available workers.
     */
    private void startProducers() {
        int workerCount = clients.size();
        // Distribute producers to workers
        int totalProducers = proof.getProducers();
        int totalKeys = proof.getKeys();
        int totalMsgRate = proof.getMsgRate();

        // Calculate base values per worker
        int baseProducersPerWorker = totalProducers / workerCount;
        int producerRemainder = totalProducers % workerCount;
        int baseKeysPerWorker = totalKeys / workerCount;
        int keysRemainder = totalKeys % workerCount;
        int baseMsgRatePerWorker = totalMsgRate / workerCount;
        int msgRateRemainder = totalMsgRate % workerCount;
        String driverName = proof.getDrivers() == null ? proof.getDriver() : proof.getDrivers().producer();
        for (int i = 0; i < clients.size(); i++) {
            int producerCount = baseProducersPerWorker + (i == workerCount - 1 ? producerRemainder : 0);
            int keyCount = baseKeysPerWorker + (i == workerCount - 1 ? keysRemainder : 0);
            int msgRate = baseMsgRatePerWorker + (i == workerCount - 1 ? msgRateRemainder : 0);
            NewProducers record = new NewProducers(
                    proof.getId(),
                    proof.getTopic(),
                    producerCount,
                    keyCount,
                    msgRate,
                    proof.getMessageSize(),
                    driverName,
                    configs.drivers().get(driverName),
                    proof.getFeatures().contains("exactly_once")
            );
            try {
                clients.get(i).startProducers(record).join();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Schedules periodic checkpoint verification.
     * This method runs at fixed intervals to:
     * <ul>
     *   <li>Collect checkpoints from all workers</li>
     *   <li>Aggregate producer and consumer checkpoints</li>
     *   <li>Verify message delivery guarantees</li>
     *   <li>Track verification successes and failures</li>
     * </ul>
     */
    private void scheduleCheckpoint() {
        executor.scheduleAtFixedRate(() -> {
            try {
                if (!running.get()) {
                    return;
                }
                // Check if the proof task has reached its duration limit
                if (proof.getDuration() > 0 && proof.getStartTime() != null) {
                    // Parse the formatted timestamp back to a LocalDateTime
                    LocalDateTime startDateTime = LocalDateTime.parse(
                            proof.getStartTime(), DateTimeFormatter.ISO_LOCAL_DATE_TIME);

                    // Calculate elapsed time
                    long elapsedSeconds = Duration.between(
                            startDateTime, LocalDateTime.now()).getSeconds();

                    if (elapsedSeconds >= proof.getDuration()) {
                        completeProofAfterDuration();
                        return;
                    }
                }

                Pair<ProducerCheckpoint, ConsumerCheckPoint> checkpoints = aggregateCheckpoints();
                ProofTask.this.latestProducerCheckpoint = checkpoints.getLeft();
                ProofTask.this.latestConsumerCheckpoint = checkpoints.getRight();
                if (ProofTask.this.inCheck == null) {
                    ProofTask.this.inCheck = ProofTask.this.latestProducerCheckpoint;
                    ProofTask.this.checkPointInCheckTimeStamps = System.currentTimeMillis();
                }

                if (sharedMode) {
                    verifyShared();
                    return;
                }

                boolean fulfilled = true;
                for (Map.Entry<String, LongSeq> entry : ProofTask.this.inCheck.getPublished().entrySet()) {
                    LongSeq expectedSeq = entry.getValue();
                    LongSeq actualSeq = ProofTask.this.latestConsumerCheckpoint.getLastSeq(entry.getKey());
                    if (actualSeq == null || actualSeq.compareTo(expectedSeq) < 0) {
                        log.info("[{}] checkpoint verify in progress | {} | {} <= {} ",
                            proof.getId(), entry.getKey(), expectedSeq.seq(),
                            actualSeq == null ? -1L : actualSeq.seq());
                        fulfilled = false;
                        break;
                    }
                }

                if (fulfilled) {
                    // Compute watermarks before mutating state
                    Map<String, Long> newWatermarks =
                            latestConsumerCheckpoint.computeHighWatermarks(highWatermarks);
                    log.info("[{}] checkpoint verify succeed: {}", proof.getId(), getSummary());
                    failed = false;
                    ProofTask.this.lastVerifiedProducerCheckpoint = ProofTask.this.inCheck;
                    ProofTask.this.lastVerifiedConsumerCheckpoint = ProofTask.this.latestConsumerCheckpoint;
                    highWatermarks = newWatermarks;
                    ProofTask.this.inCheck = ProofTask.this.latestProducerCheckpoint;
                    ProofTask.this.checkPointInCheckTimeStamps = System.currentTimeMillis();
                } else if (Duration.ofMillis(System.currentTimeMillis() - ProofTask.this.checkPointInCheckTimeStamps)
                        .compareTo(Duration.ofSeconds(proof.getTimeout())) > 0) {
                    log.error("[{}] checkpoint verify failed: {}", proof.getId(), getSummary());
                    failed = true;
                    ProofTask.this.lastFailedProducerCheckpoint = ProofTask.this.inCheck;
                    ProofTask.this.lastFailedConsumerCheckpoint = ProofTask.this.latestConsumerCheckpoint;
                    timeouts++;
                    ProofTask.this.checkPointInCheckTimeStamps = System.currentTimeMillis();
                }
                recordTimeSeriesPoint();
            } catch (Exception e) {
                log.error("Unexpected proof task {} failed", proof.getId(), e);
            }

        }, proof.getCheckPointInterval(), proof.getCheckPointInterval(), TimeUnit.SECONDS);
    }

    private void verifyShared() {
        if (ProofTask.this.inCheck.getPublished().isEmpty()) {
            ProofTask.this.inCheck = ProofTask.this.latestProducerCheckpoint;
            ProofTask.this.checkPointInCheckTimeStamps = System.currentTimeMillis();
        }

        Map<String, Long> newWatermarks = latestConsumerCheckpoint.computeHighWatermarks(highWatermarks);
        highWatermarks = newWatermarks;

        boolean fulfilled = true;
        for (Map.Entry<String, LongSeq> entry : inCheck.getPublished().entrySet()) {
            long expectedSeq = entry.getValue().seq();
            Long watermark = newWatermarks.get(entry.getKey());
            if (watermark == null || watermark < expectedSeq) {
                log.info("[{}] shared checkpoint verify in progress | {} | expected {} <= watermark {} ",
                        proof.getId(), entry.getKey(), expectedSeq,
                        watermark == null ? -1L : watermark);
                fulfilled = false;
                break;
            }
        }

        if (fulfilled) {
            log.info("[{}] shared checkpoint verify succeed: {}", proof.getId(), getSummary());
            failed = false;
            lastVerifiedProducerCheckpoint = inCheck;
            lastVerifiedConsumerCheckpoint = latestConsumerCheckpoint;
            inCheck = latestProducerCheckpoint;
            checkPointInCheckTimeStamps = System.currentTimeMillis();
        } else if (Duration.ofMillis(System.currentTimeMillis() - checkPointInCheckTimeStamps)
                .compareTo(Duration.ofSeconds(proof.getTimeout())) > 0) {
            log.error("[{}] shared checkpoint verify failed: {}", proof.getId(), getSummary());
            failed = true;
            lastFailedProducerCheckpoint = inCheck;
            lastFailedConsumerCheckpoint = latestConsumerCheckpoint;
            timeouts++;
            checkPointInCheckTimeStamps = System.currentTimeMillis();
        }
        recordTimeSeriesPoint();
    }

    private void completeProofAfterDuration() {
        if (!completionHandled.compareAndSet(false, true)) {
            return;
        }

        log.info("Stopping proof task {} after reaching the specified duration of {} seconds",
                proof.getId(), proof.getDuration());
        recordTimeSeriesPoint();
        sendCompletionNotification(getSummary());
        stop();
    }

    public void requestStop() {
        manuallyStopped.set(true);
        stop();
    }

    private int resolveFinalWaitSeconds() {
        Integer proofFinalWaitSeconds = proof.getFinalWaitSeconds();
        if (proofFinalWaitSeconds != null) {
            return Math.max(0, proofFinalWaitSeconds);
        }

        int configuredFinalWaitSeconds = configs.reportIntSetting(REPORT_SETTING_FINAL_WAIT_SECONDS, -1);
        if (configuredFinalWaitSeconds >= 0) {
            return configuredFinalWaitSeconds;
        }

        int legacyFinalWaitSeconds = configs.reportIntSetting(REPORT_SETTING_FINAL_VERIFICATION_TIMEOUT_SECONDS, -1);
        if (legacyFinalWaitSeconds >= 0) {
            return legacyFinalWaitSeconds;
        }

        return DEFAULT_FINAL_WAIT_SECONDS;
    }

    private void runFinalVerification() {
        int maxRetries = resolveFinalWaitSeconds();
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                Pair<ProducerCheckpoint, ConsumerCheckPoint> checkpoints = aggregateCheckpoints();
                latestProducerCheckpoint = checkpoints.getLeft();
                latestConsumerCheckpoint = checkpoints.getRight();
                inCheck = latestProducerCheckpoint;

                boolean fulfilled;
                if (sharedMode) {
                    Map<String, Long> newWatermarks =
                            latestConsumerCheckpoint.computeHighWatermarks(highWatermarks);
                    highWatermarks = newWatermarks;

                    fulfilled = true;
                    for (Map.Entry<String, LongSeq> entry : inCheck.getPublished().entrySet()) {
                        long expectedSeq = entry.getValue().seq();
                        Long watermark = newWatermarks.get(entry.getKey());
                        if (watermark == null || watermark < expectedSeq) {
                            fulfilled = false;
                            break;
                        }
                    }
                    if (fulfilled) {
                        lastVerifiedProducerCheckpoint = inCheck;
                        lastVerifiedConsumerCheckpoint = latestConsumerCheckpoint;
                    }
                } else {
                    fulfilled = true;
                    for (Map.Entry<String, LongSeq> entry : inCheck.getPublished().entrySet()) {
                        LongSeq expectedSeq = entry.getValue();
                        LongSeq actualSeq = latestConsumerCheckpoint.getLastSeq(entry.getKey());
                        if (actualSeq == null || actualSeq.compareTo(expectedSeq) < 0) {
                            fulfilled = false;
                            break;
                        }
                    }
                    if (fulfilled) {
                        Map<String, Long> newWatermarks =
                                latestConsumerCheckpoint.computeHighWatermarks(highWatermarks);
                        lastVerifiedProducerCheckpoint = inCheck;
                        lastVerifiedConsumerCheckpoint = latestConsumerCheckpoint;
                        highWatermarks = newWatermarks;
                    }
                }
                if (fulfilled) {
                    log.info("[{}] Final verification succeeded on attempt {}", proof.getId(), attempt + 1);
                    return;
                }
                if (attempt < maxRetries) {
                    log.info("[{}] Final verification waiting for consumers to catch up (attempt {}/{})",
                            proof.getId(), attempt + 1, maxRetries);
                    Thread.sleep(1000);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Final verification interrupted for proof {}", proof.getId());
                return;
            } catch (Exception e) {
                log.warn("Final checkpoint verification failed for proof {}", proof.getId(), e);
                return;
            }
        }
        log.warn("[{}] Final verification: consumers did not catch up within {} seconds",
                proof.getId(), maxRetries);
    }

    Pair<ProducerCheckpoint, ConsumerCheckPoint> aggregateCheckpoints() {
        ProducerCheckpoint aggregatedProducerCheckpoint = new ProducerCheckpoint();
        ConsumerCheckPoint aggregatedConsumerCheckpoint = new ConsumerCheckPoint();
        for (WorkerHttpClient client : clients) {
            try {
                ProducerCheckpoint producerCheckpoint = client.producerCheckpoint(proof.getId()).join();
                aggregatedProducerCheckpoint.merge(producerCheckpoint);
                ConsumerCheckPoint consumerCheckpoint = client.consumerCheckpoint(proof.getId(),
                        highWatermarks).join();
                aggregatedConsumerCheckpoint.merge(consumerCheckpoint);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return Pair.of(aggregatedProducerCheckpoint, aggregatedConsumerCheckpoint);
    }

    /**
     * Computes the current verified message count using the same rule as the
     * summary, selecting the shared-mode or default formula.
     */
    private long computeVerified() {
        if (sharedMode) {
            return highWatermarks.values().stream()
                    .filter(w -> w >= 0)
                    .mapToLong(w -> w + 1)
                    .sum();
        }
        return this.getLastVerifiedProducerCheckpoint().getPublished().values().stream()
                .mapToLong(s -> s.seq() + 1).sum();
    }

    /**
     * Updates the verification-stall tracking once per checkpoint tick.
     * The timer resets only when the verified count strictly increases; a flat
     * or decreasing count leaves the timer running, while the observed baseline
     * always advances to the latest value so a later rise from a low point still
     * counts as progress.
     */
    private void trackVerifiedStall(long currentVerified) {
        long now = System.currentTimeMillis();
        long stallMillis = now - lastVerifiedChangeMillis;
        if (stallMillis > maxVerifiedStallMillis) {
            maxVerifiedStallMillis = stallMillis;
        }
        if (currentVerified > lastVerifiedObserved) {
            lastVerifiedChangeMillis = now;
        }
        lastVerifiedObserved = currentVerified;
    }

    /**
     * Wall-clock seconds since the verified count last increased. While the run
     * is active this grows with the clock; once stopped it is frozen at the
     * completion time so historical reports stay stable.
     */
    private long computeVerifiedStallSeconds() {
        long end = running.get() ? System.currentTimeMillis() : completionMillis;
        return Math.max(0L, (end - lastVerifiedChangeMillis) / 1000);
    }

    /**
     * Peak verification stall in seconds. Folds the live gap into the recorded
     * peak so the value is correct between ticks, and is frozen at completion.
     */
    private long computeMaxVerifiedStallSeconds() {
        long end = running.get() ? System.currentTimeMillis() : completionMillis;
        long liveStallMillis = end - lastVerifiedChangeMillis;
        return Math.max(0L, Math.max(maxVerifiedStallMillis, liveStallMillis)) / 1000;
    }

    /**
     * Generates a summary of the proof test execution.
     *
     * @return A ProofSummary containing verification statistics
     */
    public ProofSummary getSummary() {
        if (sharedMode) {
            long verified = computeVerified();
            ConsumerCheckPoint cp = this.getLastVerifiedConsumerCheckpoint();
            cp.calculate();
            return new ProofSummary(
                    verified,
                    countProducerErrors(this.getLastVerifiedProducerCheckpoint()),
                    0, // out-of-order: not applicable for Shared
                    cp.getMissedSeqs().values().stream()
                            .flatMap(ranges -> ranges.stream()
                                    .map(range -> range.getEnd().seq() - range.getStart().seq() - 1))
                            .mapToInt(Long::intValue)
                            .sum(),
                    cp.getDuplicatedCount().values().stream().reduce(0L, Long::sum),
                    0, // write duplicates: not tracked in shared mode
                    this.getTimeouts(),
                    computeVerifiedStallSeconds(),
                    computeMaxVerifiedStallSeconds());
        }
        long verified = computeVerified();
        ConsumerCheckPoint lastVerifiedConsumerCheckpoint = this.getLastVerifiedConsumerCheckpoint();
        lastVerifiedConsumerCheckpoint.calculate();
        return new ProofSummary(
                verified,
                countProducerErrors(this.getLastVerifiedProducerCheckpoint()),
                lastVerifiedConsumerCheckpoint.getOutOfOrderSeqs().values().stream()
                        .flatMap(ranges -> ranges.stream()
                                .map(range -> range.getRight() - range.getLeft() + 1))
                        .mapToInt(Long::intValue)
                        .sum(),
                lastVerifiedConsumerCheckpoint.getMissedSeqs().values().stream()
                        .flatMap(ranges -> ranges.stream()
                                .map(range -> range.getEnd().seq() - range.getStart().seq() - 1))
                        .mapToInt(Long::intValue)
                        .sum(),
                lastVerifiedConsumerCheckpoint.getDuplicatedCount().values().stream().reduce(0L, Long::sum),
                lastVerifiedConsumerCheckpoint.getWriteDuplicatesSeqs().values().stream()
                        .flatMap(ranges -> ranges.stream()
                                .map(range -> range.getEnd().seq() - range.getStart().seq() - 1))
                        .mapToInt(Long::intValue)
                        .sum(),
                this.getTimeouts(),
                computeVerifiedStallSeconds(),
                computeMaxVerifiedStallSeconds());
    }

    /**
     * Generates detailed proof test execution results including summary and detailed error information.
     *
     * @return A ProofDetails containing comprehensive verification data
     */
    public ProofDetails getDetails() {
        ProofSummary summary = getSummary();
        Map<String, List<LongSeq>> failedKeys = new HashMap<>();
        if (failed) {
            if (sharedMode) {
                inCheck.getPublished().forEach((k, v) -> {
                    Long watermark = highWatermarks.get(k);
                    if (watermark == null || watermark < v.seq()) {
                        failedKeys.put(k, List.of(v,
                                new LongSeq(watermark == null ? -1 : watermark, null)));
                    }
                });
            } else {
                latestConsumerCheckpoint.calculate();
                inCheck.getPublished().forEach((k, v) -> {
                    LongSeq consumerLongSeq = latestConsumerCheckpoint.getLastSeq(k);
                    if (consumerLongSeq != null
                            && v.compareTo(consumerLongSeq) > 0) {
                        failedKeys.put(k, List.of(v, consumerLongSeq));
                    }
                });
            }
        }
        ConsumerCheckPoint lastVerifiedConsumerCheckpoint = this.getLastVerifiedConsumerCheckpoint();
        lastVerifiedConsumerCheckpoint.calculate();
        
        Checkpoints checkpoints = new Checkpoints(
                this.inCheck,
                this.latestProducerCheckpoint,
                this.latestConsumerCheckpoint,
                this.getLastVerifiedProducerCheckpoint(),
                lastVerifiedConsumerCheckpoint,
                this.lastFailedProducerCheckpoint,
                this.lastFailedConsumerCheckpoint
        );
        
        return new ProofDetails(
                proof,
                summary,
                checkpoints,
                failedKeys,
                lastVerifiedConsumerCheckpoint.getMissedSeqs(),
                lastVerifiedConsumerCheckpoint.getOutOfOrderSeqs(),
                lastVerifiedConsumerCheckpoint.getWriteDuplicatesSeqs());
    }

    /**
     * Generates a lightweight report for UI consumption.
     *
     * @return A compact report view of the proof run
     */
    public ProofReport getReport() {
        ProofSummary summary = getSummary();
        ProofPerformanceSummary performanceSummary = running.get()
                ? buildPerformanceSummary(summary)
                : latestPerformanceSummary != null
                        ? latestPerformanceSummary
                        : buildPerformanceSummary(summary);
        String status = determineResultStatus(summary, performanceSummary);
        String resultReason = determineResultReason(summary, performanceSummary, status);
        // Append a trailing point that extends the chart to the current elapsed time.
        // Reuse the last windowed point's rates and latency so the chart stays flat
        // instead of snapping to the cumulative average.  Only the absolute counters
        // (publishedMessages, consumedMessages, etc.) are updated to the latest values.
        List<ProofTimeSeriesPoint> reportTimeSeries = new ArrayList<>(timeSeries);
        if (!reportTimeSeries.isEmpty()) {
            ProofTimeSeriesPoint last = reportTimeSeries.getLast();
            if (last.elapsedSeconds() != performanceSummary.elapsedSeconds()) {
                reportTimeSeries.add(new ProofTimeSeriesPoint(
                        performanceSummary.elapsedSeconds(),
                        last.publishRate(),
                        last.consumeRate(),
                        performanceSummary.backlogMessages(),
                        last.publishErrorRate(),
                        last.publishLatencyP95(),
                        last.publishLatencyP99(),
                        last.endToEndLatencyP95(),
                        last.endToEndLatencyP99(),
                        last.publishBytesRate(),
                        last.consumeBytesRate(),
                        summary.verified(),
                        performanceSummary.publishedMessages(),
                        performanceSummary.consumedMessages(),
                        summary.errors(),
                        summary.timeouts(),
                        summary.missed(),
                        summary.duplicates(),
                        summary.outOfOrders()));
            }
        }
        return new ProofReport(
                proof,
                status,
                resultReason,
                resolveClusterTargets(),
                summary,
                new ProofCheckpointSummary(
                        countProducerKeys(inCheck),
                        countProducerKeys(latestProducerCheckpoint),
                        countConsumerKeys(latestConsumerCheckpoint),
                        countProducerKeys(lastVerifiedProducerCheckpoint),
                        countConsumerKeys(lastVerifiedConsumerCheckpoint),
                        countProducerKeys(lastFailedProducerCheckpoint),
                        countConsumerKeys(lastFailedConsumerCheckpoint)),
                performanceSummary,
                List.copyOf(reportTimeSeries));
    }

    private List<ProofClusterTarget> resolveClusterTargets() {
        List<ProofClusterTarget> snapshot = clusterTargetsSnapshot;
        return snapshot != null ? snapshot : buildClusterTargets();
    }

    private List<ProofClusterTarget> buildClusterTargets() {
        List<ProofClusterTarget> targets = new ArrayList<>();
        appendClusterTarget(targets, "default", proof.getDriver());

        if (proof.getDrivers() != null) {
            appendClusterTarget(targets, "admin", proof.getDrivers().admin());
            appendClusterTarget(targets, "producer", proof.getDrivers().producer());
            appendClusterTarget(targets, "consumer", proof.getDrivers().consumer());
        }
        return targets;
    }

    private void appendClusterTarget(List<ProofClusterTarget> targets, String role, String driverName) {
        if (driverName == null || driverName.isBlank()) {
            return;
        }

        Driver driverConfig = configs.drivers() != null ? configs.drivers().get(driverName) : null;
        if (driverConfig == null) {
            targets.add(new ProofClusterTarget(role, driverName, null, Map.of(), null));
            return;
        }

        targets.add(new ProofClusterTarget(
                role,
                driverName,
                driverConfig.driverType(),
                sanitizeEndpointConfig(driverConfig.driverConfigs()),
                deepCopyMap(driverConfig.metadata())));
    }

    private void freezeClusterTargetsSnapshot() {
        if (clusterTargetsSnapshot != null) {
            return;
        }
        clusterTargetsSnapshot = List.copyOf(buildClusterTargets());
    }

    private Map<String, Object> sanitizeEndpointConfig(Map<String, Object> driverConfigs) {
        if (driverConfigs == null || driverConfigs.isEmpty()) {
            return Map.of();
        }

        Map<String, Object> endpoints = new LinkedHashMap<>();
        copyEndpointValue(driverConfigs, endpoints, "pulsar.service.url");
        copyEndpointValue(driverConfigs, endpoints, "pulsar.admin.url");
        copyEndpointValue(driverConfigs, endpoints, "mqtt.service.url");
        copyEndpointValue(driverConfigs, endpoints, "bootstrap.servers");
        copyEndpointValue(driverConfigs, endpoints, "serviceUrl");
        copyEndpointValue(driverConfigs, endpoints, "adminUrl");
        return endpoints;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> deepCopyMap(Map<String, Object> source) {
        if (source == null || source.isEmpty()) {
            return null;
        }
        Map<String, Object> copy = new LinkedHashMap<>();
        source.forEach((key, value) -> copy.put(key, deepCopyValue(value)));
        return copy;
    }

    @SuppressWarnings("unchecked")
    private static Object deepCopyValue(Object value) {
        if (value instanceof Map<?, ?> mapValue) {
            Map<String, Object> nested = new LinkedHashMap<>();
            mapValue.forEach((key, nestedValue) -> nested.put(String.valueOf(key), deepCopyValue(nestedValue)));
            return nested;
        }
        if (value instanceof List<?> listValue) {
            List<Object> nested = new ArrayList<>(listValue.size());
            listValue.forEach(item -> nested.add(deepCopyValue(item)));
            return nested;
        }
        return value;
    }

    private void copyEndpointValue(Map<String, Object> source, Map<String, Object> target, String key) {
        Object value = source.get(key);
        if (value != null) {
            target.put(key, value);
        }
    }

    private static int countProducerKeys(ProducerCheckpoint checkpoint) {
        if (checkpoint == null || checkpoint.getPublished() == null) {
            return 0;
        }
        return checkpoint.getPublished().size();
    }

    private static int countConsumerKeys(ConsumerCheckPoint checkpoint) {
        if (checkpoint == null || checkpoint.getConsumed() == null) {
            return 0;
        }
        return checkpoint.getConsumed().size();
    }

    private ProofPerformanceSummary buildPerformanceSummary(ProofSummary summary) {
        ProofWorkerMetricsSnapshot workerMetrics = aggregateWorkerMetrics();
        long elapsedSeconds = getElapsedSeconds();
        long plannedDurationSeconds = Math.max(0L, proof.getDuration());
        long remainingSeconds = plannedDurationSeconds > 0
                ? Math.max(0L, plannedDurationSeconds - elapsedSeconds)
                : 0L;
        long publishedMessages = workerMetrics.acknowledgedMessages();
        long publishErrors = workerMetrics.publishErrors();
        long consumedMessages = workerMetrics.receivedMessages();
        long publishAttempts = workerMetrics.sendAttempts();
        long backlogMessages = Math.max(0L, publishedMessages - consumedMessages);
        double durationSeconds = Math.max(1L, elapsedSeconds);
        double progressPercent = proof.getDuration() > 0
                ? Math.min(100.0d, durationSeconds * 100.0d / proof.getDuration())
                : 0.0d;
        long publishedBytes = workerMetrics.acknowledgedBytes();
        long consumedBytes = workerMetrics.receivedBytes();
        return new ProofPerformanceSummary(
                elapsedSeconds,
                plannedDurationSeconds,
                remainingSeconds,
                progressPercent,
                proof.getMsgRate(),
                publishedMessages,
                publishErrors,
                publishAttempts,
                consumedMessages,
                backlogMessages,
                summary.verified(),
                publishedMessages / durationSeconds,
                consumedMessages / durationSeconds,
                publishErrors / durationSeconds,
                summary.verified() / durationSeconds,
                publishedBytes / durationSeconds,
                consumedBytes / durationSeconds,
                buildLatencySummary(workerMetrics.publishLatency()),
                buildLatencySummary(workerMetrics.endToEndLatency()));
    }

    private String determineResultStatus(ProofSummary summary, ProofPerformanceSummary performanceSummary) {
        if (startFailureReason != null) {
            return "failed";
        }
        if (running.get()) {
            return stopping.get() ? "stopping" : "running";
        }
        if (manuallyStopped.get()) {
            return "stopped";
        }
        if (summary.missed() > 0 || summary.outOfOrders() > 0) {
            return "failed";
        }
        if (proof.getMaxStallSeconds() > 0
                && summary.maxVerifiedStallSeconds() > proof.getMaxStallSeconds()) {
            return "failed";
        }
        return "passed";
    }

    private String determineResultReason(
            ProofSummary summary, ProofPerformanceSummary performanceSummary, String resultStatus) {
        if (startFailureReason != null) {
            return "The run failed to start: " + startFailureReason;
        }
        if (running.get()) {
            if (stopping.get()) {
                return "Stop requested. Final verification is still in progress.";
            }
            return "Verification is still in progress.";
        }
        if (manuallyStopped.get()) {
            return "The run was stopped manually.";
        }
        if (summary.outOfOrders() > 0) {
            return "Out-of-order messages were detected.";
        }
        if (summary.missed() > 0) {
            return "Some messages were not verified by consumers.";
        }
        if (proof.getMaxStallSeconds() > 0
                && summary.maxVerifiedStallSeconds() > proof.getMaxStallSeconds()) {
            return "Verification stalled for " + summary.maxVerifiedStallSeconds()
                    + "s, exceeding the configured limit of " + proof.getMaxStallSeconds() + "s.";
        }
        if ("passed".equals(resultStatus)) {
            return "The run completed without missed or out-of-order messages.";
        }
        return "The run completed with unresolved verification issues.";
    }

    private long getElapsedSeconds() {
        if (proof.getStartTime() == null || proof.getStartTime().isBlank()) {
            return 0L;
        }
        try {
            LocalDateTime startTime = LocalDateTime.parse(proof.getStartTime(), DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            return Math.max(0L, Duration.between(startTime, LocalDateTime.now()).toSeconds());
        } catch (Exception e) {
            log.debug("Failed to parse proof start time {}", proof.getStartTime(), e);
            return 0L;
        }
    }

    private static long countPublishedMessages(ProducerCheckpoint checkpoint) {
        if (checkpoint == null || checkpoint.getPublished() == null) {
            return 0L;
        }
        return checkpoint.getPublished().values().stream()
                .mapToLong(longSeq -> Math.max(0L, longSeq.seq() + 1))
                .sum();
    }

    private static int countProducerErrors(ProducerCheckpoint checkpoint) {
        if (checkpoint == null || checkpoint.getErrorDetails() == null) {
            return 0;
        }
        return checkpoint.getErrorDetails().values().stream()
                .mapToInt(ErrorOccurrence::getCount)
                .sum();
    }

    private static long countConsumedMessages(ConsumerCheckPoint checkpoint) {
        if (checkpoint == null || checkpoint.getConsumed() == null) {
            return 0L;
        }
        return checkpoint.getConsumed().values().stream()
                .flatMap(rangeMap -> rangeMap.values().stream())
                .mapToLong(range -> (range.getEnd().seq() - range.getStart().seq() + 1) + range.getDuplicated())
                .sum();
    }

    private ProofWorkerMetricsSnapshot aggregateWorkerMetrics() {
        long sendAttempts = 0L;
        long acknowledgedMessages = 0L;
        long acknowledgedBytes = 0L;
        long publishErrors = 0L;
        long receivedMessages = 0L;
        long receivedBytes = 0L;
        LatencyMetricSnapshot publishLatency = null;
        LatencyMetricSnapshot endToEndLatency = null;
        for (WorkerHttpClient client : clients) {
            try {
                ProofWorkerMetricsSnapshot snapshot = client.metrics(proof.getId()).join();
                sendAttempts += snapshot.sendAttempts();
                acknowledgedMessages += snapshot.acknowledgedMessages();
                acknowledgedBytes += snapshot.acknowledgedBytes();
                publishErrors += snapshot.publishErrors();
                receivedMessages += snapshot.receivedMessages();
                receivedBytes += snapshot.receivedBytes();
                publishLatency = mergeLatencySnapshots(publishLatency, snapshot.publishLatency());
                endToEndLatency = mergeLatencySnapshots(endToEndLatency, snapshot.endToEndLatency());
            } catch (Exception e) {
                log.warn("Failed to collect worker metrics for proof {}", proof.getId(), e);
            }
        }
        return new ProofWorkerMetricsSnapshot(
                sendAttempts,
                acknowledgedMessages,
                acknowledgedBytes,
                publishErrors,
                receivedMessages,
                receivedBytes,
                publishLatency,
                endToEndLatency);
    }

    /**
     * Aggregates windowed metrics from all workers.  Latency samples only
     * cover the interval since the last windowed read; counters are cumulative.
     */
    private ProofWorkerMetricsSnapshot aggregateWindowedWorkerMetrics() {
        long sendAttempts = 0L;
        long acknowledgedMessages = 0L;
        long acknowledgedBytes = 0L;
        long publishErrors = 0L;
        long receivedMessages = 0L;
        long receivedBytes = 0L;
        LatencyMetricSnapshot publishLatency = null;
        LatencyMetricSnapshot endToEndLatency = null;
        for (WorkerHttpClient client : clients) {
            try {
                ProofWorkerMetricsSnapshot snapshot = client.metricsWindowed(proof.getId()).join();
                sendAttempts += snapshot.sendAttempts();
                acknowledgedMessages += snapshot.acknowledgedMessages();
                acknowledgedBytes += snapshot.acknowledgedBytes();
                publishErrors += snapshot.publishErrors();
                receivedMessages += snapshot.receivedMessages();
                receivedBytes += snapshot.receivedBytes();
                publishLatency = mergeLatencySnapshots(publishLatency, snapshot.publishLatency());
                endToEndLatency = mergeLatencySnapshots(endToEndLatency, snapshot.endToEndLatency());
            } catch (Exception e) {
                log.warn("Failed to collect windowed worker metrics for proof {}", proof.getId(), e);
            }
        }
        return new ProofWorkerMetricsSnapshot(
                sendAttempts,
                acknowledgedMessages,
                acknowledgedBytes,
                publishErrors,
                receivedMessages,
                receivedBytes,
                publishLatency,
                endToEndLatency);
    }

    private static LatencyMetricSnapshot mergeLatencySnapshots(
            LatencyMetricSnapshot left, LatencyMetricSnapshot right) {
        if (left == null) {
            return right;
        }
        if (right == null) {
            return left;
        }
        List<Long> mergedSamples = new ArrayList<>();
        if (left.samplesMillis() != null) {
            mergedSamples.addAll(left.samplesMillis());
        }
        if (right.samplesMillis() != null) {
            mergedSamples.addAll(right.samplesMillis());
        }
        return new LatencyMetricSnapshot(
                left.count() + right.count(),
                left.sumMillis() + right.sumMillis(),
                Math.max(left.maxMillis(), right.maxMillis()),
                mergedSamples);
    }

    private static LatencySummary buildLatencySummary(LatencyMetricSnapshot snapshot) {
        if (snapshot == null || snapshot.count() <= 0) {
            return new LatencySummary(0L, 0.0d, 0.0d, 0.0d, 0.0d, 0.0d);
        }
        List<Long> sortedSamples = new ArrayList<>();
        if (snapshot.samplesMillis() != null) {
            sortedSamples.addAll(snapshot.samplesMillis().stream().filter(v -> v != null).toList());
        }
        sortedSamples.sort(Comparator.naturalOrder());
        return new LatencySummary(
                snapshot.count(),
                snapshot.sumMillis() / Math.max(1L, snapshot.count()),
                percentile(sortedSamples, 0.50d),
                percentile(sortedSamples, 0.95d),
                percentile(sortedSamples, 0.99d),
                snapshot.maxMillis());
    }

    private static double percentile(List<Long> sortedSamples, double quantile) {
        if (sortedSamples.isEmpty()) {
            return 0.0d;
        }
        int index = (int) Math.ceil(quantile * sortedSamples.size()) - 1;
        index = Math.max(0, Math.min(index, sortedSamples.size() - 1));
        return sortedSamples.get(index);
    }

    private long timeSeriesSampleInterval() {
        long checkpoint = proof.getCheckPointInterval();
        if (checkpoint <= 0) {
            checkpoint = 5;
        }
        int custom = proof.getTimeSeriesInterval();
        if (custom > 0) {
            return ((Math.max(custom, checkpoint) + checkpoint - 1) / checkpoint) * checkpoint;
        }
        int maxPoints = configs.reportIntSetting("maxTimeSeriesPoints", 481);
        long duration = proof.getDuration();
        if (duration <= 0) {
            return checkpoint;
        }
        long raw = Math.max(checkpoint, duration / maxPoints);
        return ((raw + checkpoint - 1) / checkpoint) * checkpoint;
    }

    private void recordTimeSeriesPoint() {
        trackVerifiedStall(computeVerified());
        ProofSummary summary = getSummary();
        ProofPerformanceSummary performanceSummary = buildPerformanceSummary(summary);
        latestPerformanceSummary = performanceSummary;

        // Skip this point if not enough time has passed since the last one
        long sampleInterval = timeSeriesSampleInterval();
        if (!timeSeries.isEmpty()) {
            ProofTimeSeriesPoint lastPoint = timeSeries.getLast();
            if (lastPoint.elapsedSeconds() == performanceSummary.elapsedSeconds()) {
                timeSeries.removeLast();
            } else if (performanceSummary.elapsedSeconds() - lastPoint.elapsedSeconds() < sampleInterval) {
                return;
            }
        }

        // Compute windowed rates from counter deltas
        long deltaSeconds = performanceSummary.elapsedSeconds() - prevElapsedSeconds;
        double windowDuration = Math.max(1.0d, deltaSeconds);

        double windowedPublishRate =
                (performanceSummary.publishedMessages() - prevPublishedMessages) / windowDuration;
        double windowedConsumeRate =
                (performanceSummary.consumedMessages() - prevConsumedMessages) / windowDuration;
        double windowedPublishErrorRate =
                (performanceSummary.publishErrors() - prevPublishErrors) / windowDuration;

        // Fetch windowed latency from workers (reset-on-read).
        // The windowed endpoint returns cumulative counters plus windowed latency,
        // so we can also use its acknowledgedBytes/receivedBytes for byte-rate deltas.
        ProofWorkerMetricsSnapshot windowedMetrics = aggregateWindowedWorkerMetrics();
        double windowedPublishBytesRate =
                (windowedMetrics.acknowledgedBytes() - prevPublishedBytes) / windowDuration;
        double windowedConsumeBytesRate =
                (windowedMetrics.receivedBytes() - prevConsumedBytes) / windowDuration;
        LatencySummary windowedPublishLatency = buildLatencySummary(windowedMetrics.publishLatency());
        LatencySummary windowedE2eLatency = buildLatencySummary(windowedMetrics.endToEndLatency());

        timeSeries.add(buildTimeSeriesPoint(
                performanceSummary, summary,
                windowedPublishRate, windowedConsumeRate, windowedPublishErrorRate,
                windowedPublishBytesRate, windowedConsumeBytesRate,
                windowedPublishLatency, windowedE2eLatency));

        // Update previous counters for next window
        prevElapsedSeconds = performanceSummary.elapsedSeconds();
        prevPublishedMessages = performanceSummary.publishedMessages();
        prevConsumedMessages = performanceSummary.consumedMessages();
        prevPublishErrors = performanceSummary.publishErrors();
        prevPublishedBytes = windowedMetrics.acknowledgedBytes();
        prevConsumedBytes = windowedMetrics.receivedBytes();
    }

    private ProofTimeSeriesPoint buildTimeSeriesPoint(
            ProofPerformanceSummary performanceSummary, ProofSummary summary,
            double publishRate, double consumeRate, double publishErrorRate,
            double publishBytesRate, double consumeBytesRate,
            LatencySummary publishLatency, LatencySummary endToEndLatency) {
        return new ProofTimeSeriesPoint(
                performanceSummary.elapsedSeconds(),
                publishRate,
                consumeRate,
                performanceSummary.backlogMessages(),
                publishErrorRate,
                publishLatency == null ? 0.0d : publishLatency.p95(),
                publishLatency == null ? 0.0d : publishLatency.p99(),
                endToEndLatency == null ? 0.0d : endToEndLatency.p95(),
                endToEndLatency == null ? 0.0d : endToEndLatency.p99(),
                publishBytesRate,
                consumeBytesRate,
                summary.verified(),
                performanceSummary.publishedMessages(),
                performanceSummary.consumedMessages(),
                summary.errors(),
                summary.timeouts(),
                summary.missed(),
                summary.duplicates(),
                summary.outOfOrders());
    }

    /**
     * Deletes the messaging system topic and performs cleanup.
     */
    public void remove() {
        try {
            deleteProofTopics();
        } finally {
            closeTopicDrivers();
        }
        log.info("ProofTask {} removed", proof.getId());
    }

    /**
     * Sends webhook notification when proof test completes with results.
     *
     * @param summary The proof execution summary
     */
    public void sendCompletionNotification(ProofSummary summary) {
        if (proof.getWebhookConfig() != null && proof.getWebhookConfig().isEnabled()) {
            try {
                webhookService.sendProofCompletionNotification(proof, summary)
                        .whenComplete((result, throwable) -> {
                            if (throwable != null) {
                                log.error("Failed to send completion webhook notification for proof {}",
                                        proof.getId(), throwable);
                            } else {
                                log.info("Successfully sent completion webhook notification for proof {}",
                                        proof.getId());
                            }
                        });
            } catch (Exception e) {
                log.error("Error sending completion webhook notification for proof {}",
                        proof.getId(), e);
            }
        }
    }

    /**
     * Stops the proof test execution.
     * Stops all producers and shuts down the executor service.
     */
    public void stop() {
        if (!stopping.compareAndSet(false, true)) {
            return;
        }
        freezeClusterTargetsSnapshot();

        executor.shutdown();

        // Stop producers first so no more messages are sent.  The producer
        // state remains on the worker until consumers are stopped, so the
        // subsequent checkpoint can still read the final published sequences.
        clients.forEach(client -> {
            try {
                client.stopProducers(proof.getId()).join();
            } catch (Exception e) {
                log.warn("Failed to stop producers for proof {}", proof.getId(), e);
            }
        });

        // Run final verification after producers have stopped but while
        // consumers are still active, capturing every published message.
        runFinalVerification();

        // Record a final time-series point so the chart reflects the
        // fully-verified state after all consumers have caught up.
        recordTimeSeriesPoint();

        // Build the final performance snapshot after verification has
        // settled, while worker metrics are still available.
        latestPerformanceSummary = buildPerformanceSummary(getSummary());

        clients.forEach(client -> {
            try {
                client.stopAndRemoveConsumers(proof.getId()).join();
            } catch (Exception e) {
                log.warn("Failed to stop consumers for proof {}", proof.getId(), e);
            }
        });

        if (webhookService != null) {
            webhookService.close();
        }

        // Mark as stopped only after all cleanup is complete, so the
        // report API never returns "stopped" with stale data.
        completionMillis = System.currentTimeMillis();
        running.set(false);
        log.info("ProofTask {} stopped", proof.getId());
    }
}
