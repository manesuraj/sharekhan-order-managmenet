package com.myproject.OrderManagment.service;

import com.myproject.OrderManagment.domain.*;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.myproject.OrderManagment.topology.OrdersTopology.*;
import static com.myproject.OrderManagment.topology.OrdersTopology.RESTAURANT_ORDERS_COUNT;

@Service
public class OrderWindowsService {

    private OrderStoreService orderStoreService;

    public OrderWindowsService(OrderStoreService orderStoreService) {
        this.orderStoreService = orderStoreService;
    }

    public List<OrdersCountPerStoreByWindowsDTO> getOrderCount(String orderType) {
        var orderCountStore = getCountWindowStore(orderType);
        var orderCount = orderCountStore.all();

        var spliterator = Spliterators.spliteratorUnknownSize(orderCount, 0);

        return StreamSupport.stream(spliterator, false)
                .map(stringLongKeyValue -> new OrdersCountPerStoreByWindowsDTO(
                        stringLongKeyValue.key.key(),
                        stringLongKeyValue.value,
                        mapOrderType(orderType),
                        LocalDateTime.ofInstant(stringLongKeyValue.key.window().startTime(), ZoneId.of("GMT")),
                        LocalDateTime.ofInstant(stringLongKeyValue.key.window().endTime(), ZoneId.of("GMT"))
                        ))
                .collect(Collectors.toList());
    }

    public List<OrdersCountPerStoreByWindowsDTO> getAllOrderCountForWindow() {

        var orderCountForGeneral =
                getOrderCount(GENERAL_ORDERS);

        var orderCountForRestaurant =
                getOrderCount(RESTAURANT_ORDERS);

        return Stream.of(orderCountForGeneral, orderCountForRestaurant)
                        .flatMap(Collection::stream).collect(Collectors.toList());

    }

    public List<OrdersCountPerStoreByWindowsDTO> getAllOrderCountForWindow(LocalDateTime fromTime, LocalDateTime toTime) {

        var fromTimeInstance = fromTime.toInstant(ZoneOffset.UTC);
        var toTimeInstance = toTime.toInstant(ZoneOffset.UTC);

        var orderCountForGeneral =
                getCountWindowStore(GENERAL_ORDERS).fetchAll(fromTimeInstance, toTimeInstance);

        var orderCountForRestaurant =
                getCountWindowStore(RESTAURANT_ORDERS).fetchAll(fromTimeInstance, toTimeInstance);

        var orderCountForGeneralTime = getOrderCountFromTimeStamp(orderCountForGeneral, GENERAL_ORDERS);

        var orderCountForRestaurantTime = getOrderCountFromTimeStamp(orderCountForRestaurant, RESTAURANT_ORDERS);

        return Stream.of(orderCountForGeneralTime, orderCountForRestaurantTime)
                .flatMap(Collection::stream).collect(Collectors.toList());
    }

    private List<OrdersCountPerStoreByWindowsDTO> getOrderCountFromTimeStamp(KeyValueIterator<Windowed<String>, Long> orderCountForGeneral, String generalOrders) {

        var orderCountStore = orderCountForGeneral;

        var spliterator = Spliterators.spliteratorUnknownSize(orderCountStore, 0);

        return StreamSupport.stream(spliterator, false)
                .map(stringLongKeyValue -> new OrdersCountPerStoreByWindowsDTO(
                        stringLongKeyValue.key.key(),
                        stringLongKeyValue.value,
                        mapOrderType(generalOrders),
                        LocalDateTime.ofInstant(stringLongKeyValue.key.window().startTime(), ZoneId.of("GMT")),
                        LocalDateTime.ofInstant(stringLongKeyValue.key.window().endTime(), ZoneId.of("GMT"))
                ))
                .collect(Collectors.toList());

    }

    public List<OrdersRevenuePerStoreByWindowsDTO> getAllOrderRevenueForWindow(String orderType) {
        var revenueStore = getRevenueWindowStore(orderType);
        var revenue = revenueStore.all();
        var spliterator = Spliterators.spliteratorUnknownSize(revenue, 0);
        return StreamSupport
                .stream(spliterator, false)
                .map(stringTotalRevenueKeyValue -> new OrdersRevenuePerStoreByWindowsDTO(
                        stringTotalRevenueKeyValue.key.key(),
                        stringTotalRevenueKeyValue.value,
                        mapOrderType(orderType),
                        LocalDateTime.ofInstant(stringTotalRevenueKeyValue.key.window().startTime(), ZoneId.of("GMT")),
                        LocalDateTime.ofInstant(stringTotalRevenueKeyValue.key.window().endTime(), ZoneId.of("GMT"))
                        )).collect(Collectors.toList());
    }

    public List<OrdersRevenuePerStoreByWindowsDTO> getAllRevenueForWindow() {

        var revenueStoreForGeneral = getRevenueWindowStore(GENERAL_ORDERS);
        var revenueStoreForRestaurant = getRevenueWindowStore(RESTAURANT_ORDERS);

        var revenueForGeneral = getOrderRevenueForWindow(revenueStoreForGeneral.all(), OrderType.GENERAL);
        var revenueForRestaurant = getOrderRevenueForWindow(revenueStoreForRestaurant.all(), OrderType.RESTAURANT);

        return Stream.of(revenueForGeneral, revenueForRestaurant)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }



    public List<OrdersRevenuePerStoreByWindowsDTO> getAllRevenueForWindowWithinTime(LocalDateTime fromDate, LocalDateTime toDate) {
        var generalStoreWindow = getRevenueWindowStore(GENERAL_ORDERS)
                .fetchAll(fromDate.toInstant(ZoneOffset.UTC), toDate.toInstant(ZoneOffset.UTC));

        var restaurantStoreWindow = getRevenueWindowStore(RESTAURANT_ORDERS)
                .fetchAll(fromDate.toInstant(ZoneOffset.UTC), toDate.toInstant(ZoneOffset.UTC));

        var generalStore = getOrderRevenueForWindow(generalStoreWindow, OrderType.GENERAL);
        var restaurantStore = getOrderRevenueForWindow(restaurantStoreWindow, OrderType.RESTAURANT);

        return Stream.of(generalStore, restaurantStore)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    private List<OrdersRevenuePerStoreByWindowsDTO> getOrderRevenueForWindow(KeyValueIterator<Windowed<String>, TotalRevenue> all, OrderType orderType) {

        var spliterator = Spliterators.spliteratorUnknownSize(all, 0);
        return StreamSupport
                .stream(spliterator, false)
                .map(stringTotalRevenueKeyValue -> new OrdersRevenuePerStoreByWindowsDTO(
                        stringTotalRevenueKeyValue.key.key(),
                        stringTotalRevenueKeyValue.value,
                        orderType,
                        LocalDateTime.ofInstant(stringTotalRevenueKeyValue.key.window().startTime(), ZoneId.of("GMT")),
                        LocalDateTime.ofInstant(stringTotalRevenueKeyValue.key.window().endTime(), ZoneId.of("GMT"))
                )).collect(Collectors.toList());
    }



    private ReadOnlyWindowStore<String, Long> getCountWindowStore(String orderType) {

        return switch (orderType){

            case GENERAL_ORDERS ->  orderStoreService.orderCountWindowStore(GENERAL_ORDERS_COUNT_WINDOWS);

            case RESTAURANT_ORDERS -> orderStoreService.orderCountWindowStore(RESTAURANT_ORDERS_COUNT_WINDOWS);

            default -> throw new IllegalStateException("Not a valid option");
        };
    }

    private ReadOnlyWindowStore<String, TotalRevenue> getRevenueWindowStore(String orderType) {

        return switch (orderType){

            case GENERAL_ORDERS ->  orderStoreService.orderRevenueWindowStore(GENERAL_ORDERS_REVENUE_WINDOWS);

            case RESTAURANT_ORDERS -> orderStoreService.orderRevenueWindowStore(RESTAURANT_ORDERS_REVENUE_WINDOWS);

            default -> throw new IllegalStateException("Not a valid option");
        };

    }
    private OrderType mapOrderType(String orderType) {
        return switch (orderType){
            case GENERAL_ORDERS -> OrderType.GENERAL;
            case RESTAURANT_ORDERS -> OrderType.RESTAURANT;
            default -> throw new IllegalStateException("Invalid Order Type");
        };
    }


}
