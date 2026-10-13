package org.milk.boiling.notification.router.subscriptions.cache;

import lombok.extern.slf4j.Slf4j;
import org.milk.boiling.notification.dto.SerializationService;
import org.milk.boiling.notification.router.subscriptions.UserSubscriptions;
import org.rocksdb.*;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

@Slf4j
@Component
public class RocksDbCache implements SubscriptionCache, SmartLifecycle {


    private final RocksDB rocksDB;
    private final SerializationService serializationService;
    private volatile boolean running = false;

    public RocksDbCache(SerializationService serializationService) throws RocksDBException {
        this.serializationService = serializationService;
        Options options = new Options()
                .setOptimizeFiltersForHits(true)
                .setUseDirectReads(true).setDisableAutoCompactions(true).optimizeForSmallDb()

                .setCompressionOptions(new CompressionOptions().setEnabled(false))
                .setCreateIfMissing(true);
        this.rocksDB = RocksDB.open(options, "123.db");
        UUID uuid = UUID.randomUUID();
        this.putRow(uuid, List.of(uuid.toString()));
        log.error(this.getSubscriptionPods(uuid).toString());
    }

    public List<UserSubscriptions> getSubscriptionPods(List<UUID> uuids) {
        try (var a = new ReadOptions().setAsyncIo(true)) {
            return rocksDB.multiGetAsList(a, uuids.stream().map(x -> x.toString().getBytes()).toList())
                    .stream()
                    .map(x -> (UserSubscriptions) serializationService.deserialize(x))
                    .toList();
        } catch (RocksDBException e) {
            log.error(e.getMessage());
            return Collections.emptyList();
        }
    }

    public void putRow(UUID uuids, List<String> row) {
        try {
            rocksDB.put(uuids.toString().getBytes(), serializationService.serialize(row));
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> getSubscriptionPods(UUID uuid) {
        try (var a = new ReadOptions().setAsyncIo(true)) {

            return (List<String>) serializationService.deserialize(rocksDB.get(a, uuid.toString().getBytes()));
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public UserSubscriptions updateSubscriptionPods(UUID uuid) {
        return null;
    }

    @Override
    public void start() {
        running = true;
    }

    @Override
    public void stop() {
        rocksDB.close();
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
