package com.myproject.OrderManagment.topology;

import com.myproject.OrderManagment.domain.*;
import com.myproject.OrderManagment.util.OrderTimeStampExtractor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class OrdersTopology {

    public static final String ORDERS = "orders";
    public static final String GENERAL_ORDERS = "general_orders";
    public static final String GENERAL_ORDERS_COUNT = "general_orders_count";
    public static final String GENERAL_ORDERS_COUNT_WINDOWS = "general_orders_count_window";
    public static final String GENERAL_ORDERS_REVENUE = "general_orders_revenue";
    public static final String GENERAL_ORDERS_REVENUE_WINDOWS = "general_orders_revenue_window";

    public static final String RESTAURANT_ORDERS = "restaurant_orders";
    public static final String RESTAURANT_ORDERS_COUNT = "restaurant_orders_count";
    public static final String RESTAURANT_ORDERS_REVENUE = "restaurant_orders_revenue";
    public static final String RESTAURANT_ORDERS_COUNT_WINDOWS = "restaurant_orders_count_window";
    public static final String RESTAURANT_ORDERS_REVENUE_WINDOWS = "restaurant_orders_revenue_window";
    public static final String STORES = "stores";


    private static final Logger LOGGER = LoggerFactory.getLogger(OrdersTopology.class);



    @Autowired
    public void process(StreamsBuilder streamsBuilder) {

        orderTopology(streamsBuilder);

    }

    private static void orderTopology(StreamsBuilder streamsBuilder) {

        Predicate<String, Order> generalPredicate = (key, order) -> order.orderType().equals(OrderType.GENERAL);
        Predicate<String, Order> restaurantPredicate = (key, order) -> order.orderType().equals(OrderType.RESTAURANT);

        var orderStreams = streamsBuilder
                .stream(ORDERS,
                        Consumed.with(Serdes.String(), new JsonSerde<>(Order.class))
                                .withTimestampExtractor(new OrderTimeStampExtractor())
                )
                .selectKey((key, value) -> value.locationId());

        var storesTable = streamsBuilder
                .table(STORES,
                        Consumed.with(Serdes.String(), new JsonSerde<>(Store.class)));

        storesTable
                .toStream()
                .print(Printed.<String,Store>toSysOut().withLabel("stores"));

        orderStreams
                .print(Printed.<String, Order>toSysOut().withLabel("orders"));

        orderStreams
                .split(Named.as("General-restaurant-stream"))
                .branch(generalPredicate,
                        Branched.withConsumer(generalOrdersStream -> {
                            aggregateOrdersByCount(generalOrdersStream, GENERAL_ORDERS_COUNT, storesTable);
                            aggregateOrdersCountByTimeWindows(generalOrdersStream, GENERAL_ORDERS_COUNT_WINDOWS, storesTable);
                            aggregateOrdersByRevenue(generalOrdersStream, GENERAL_ORDERS_REVENUE, storesTable);
                            aggregateOrdersRevenueByWindows(generalOrdersStream, GENERAL_ORDERS_REVENUE_WINDOWS, storesTable);

                        }))
                .branch(restaurantPredicate,
                        Branched.withConsumer(restaurantOrdersStream -> {
                            aggregateOrdersByCount(restaurantOrdersStream, RESTAURANT_ORDERS_COUNT, storesTable);
                            aggregateOrdersCountByTimeWindows(restaurantOrdersStream, RESTAURANT_ORDERS_COUNT_WINDOWS, storesTable);
                            aggregateOrdersByRevenue(restaurantOrdersStream, RESTAURANT_ORDERS_REVENUE, storesTable);
                            aggregateOrdersRevenueByWindows(restaurantOrdersStream, RESTAURANT_ORDERS_REVENUE_WINDOWS, storesTable);
                        }));
    }

    private static void aggregateOrdersCountByTimeWindows(KStream<String, Order> generalOrdersStream, String storeName, KTable<String, Store> storeKTable) {

        Duration windowSize = Duration.ofSeconds(15);
        TimeWindows timeWindow = TimeWindows.ofSizeWithNoGrace(windowSize);

        KTable<Windowed<String>, Long> generalOrdersCount = generalOrdersStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value))
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<Order>(Order.class)))
                .windowedBy(timeWindow)
                .count(Named.as(storeName),
                        Materialized.as(storeName))
                .suppress(Suppressed
                        .untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull())
                );

        generalOrdersCount
                .toStream()
                .peek(((key, value) -> {
                    LOGGER.info(" {} : tumblingWindow : key : {}, value : {}",storeName, key, value);
                }))
                .print(Printed.<Windowed<String>, Long>toSysOut().withLabel(storeName));

    }

    private static void aggregateOrdersByCount(KStream<String, Order> generalOrdersStream, String storeName, KTable<String, Store> kTable) {

        KTable<String, Long> generalOrdersCount = generalOrdersStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value))
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<Order>(Order.class)))
                .count(Named.as(storeName),
                        Materialized.as(storeName));

        generalOrdersCount
                .toStream()
                .print(Printed.<String,Long>toSysOut().withLabel(storeName));

        ValueJoiner<Long, Store, TotalCountWithAddress> valueJoiner =
                TotalCountWithAddress :: new;

        var revenueWithStoreTable = generalOrdersCount.join(kTable, valueJoiner);

        revenueWithStoreTable.toStream().print(Printed.<String, TotalCountWithAddress>toSysOut().withLabel(storeName));

    }

    private static void aggregateOrdersByRevenue(KStream<String, Order> generalOrdersStream, String aggregateStoreName, KTable<String, Store> storesTable) {


        Initializer<TotalRevenue> totalRevenueInitializer = TotalRevenue::new;

        Aggregator<String, Order, TotalRevenue> aggregator   = (key,order, totalRevenue )-> {
            return totalRevenue.updateRunningRevenue(key, order);
        };

        var revenueTable = generalOrdersStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value))
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<Order>(Order.class)))
                .aggregate(totalRevenueInitializer,
                        aggregator,
                        Materialized
                                .<String, TotalRevenue, KeyValueStore<Bytes, byte[]>>as(aggregateStoreName)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonSerde<>(TotalRevenue.class))
                );

        revenueTable
                .toStream()
                .print(Printed.<String,TotalRevenue>toSysOut().withLabel(aggregateStoreName));

        ValueJoiner<TotalRevenue, Store, TotalRevenueWithAddress> valueJoiner = TotalRevenueWithAddress::new;

        var revenueWithStoreTable = revenueTable
                .join(storesTable,valueJoiner);

        revenueWithStoreTable
                .toStream()
                .print(Printed.<String,TotalRevenueWithAddress>toSysOut().withLabel(aggregateStoreName+"-bystore"));
    }

    private static void aggregateOrdersRevenueByWindows(KStream<String, Order> generalOrdersStream, String aggregateStoreName, KTable<String, Store> storesTable) {

        Duration windowSize = Duration.ofSeconds(15);
        Duration graceWindowsSize = Duration.ofSeconds(5);

        TimeWindows timeWindow = TimeWindows.ofSizeAndGrace(windowSize, graceWindowsSize);
        Initializer<TotalRevenue> alphabetWordAggregateInitializer = TotalRevenue::new;

        Aggregator<String, Order, TotalRevenue> aggregator   = (key,order, totalRevenue )-> {
            return totalRevenue.updateRunningRevenue(key, order);
        };

        var revenueTable = generalOrdersStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value))
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<Order>(Order.class)))
                .windowedBy(timeWindow)
                .aggregate(alphabetWordAggregateInitializer,
                        aggregator
                        ,Materialized
                                .<String, TotalRevenue, WindowStore<Bytes, byte[]>>as(aggregateStoreName)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonSerde<TotalRevenue>(TotalRevenue.class))
                );

        revenueTable
                .toStream()
                .peek(((key, value) -> {
                    LOGGER.info(" {} : tumblingWindow : key : {}, value : {}",aggregateStoreName, key, value);
//                    printLocalDateTimes(key, value);
                }))
                .print(Printed.<Windowed<String>,TotalRevenue>toSysOut().withLabel(aggregateStoreName));

        ValueJoiner<TotalRevenue, Store, TotalRevenueWithAddress> valueJoiner = TotalRevenueWithAddress::new;

        var joinedParams =
                Joined.with(Serdes.String(), new JsonSerde<TotalRevenue>(TotalRevenue.class), new JsonSerde<Store>(Store.class));
        revenueTable
                .toStream()
                .peek(((key, value) -> {
                    LOGGER.info(" {} : tumblingWindow : key : {}, value : {}",aggregateStoreName, key, value);
//                    printLocalDateTimes(key, value);
                }))
                .map((key, value) -> KeyValue.pair(key.key(), value))
                .join(storesTable,valueJoiner
                        ,joinedParams
                )
                .print(Printed.<String,TotalRevenueWithAddress>toSysOut().withLabel(aggregateStoreName+"-bystore"));

    }
}
