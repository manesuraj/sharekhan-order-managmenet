package com.myproject.OrderManagment.service;


import com.myproject.OrderManagment.domain.*;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.myproject.OrderManagment.topology.OrdersTopology.*;

@Service
public class OrderService {

    private OrderStoreService orderStoreService;

    public OrderService(OrderStoreService orderStoreService) {
        this.orderStoreService = orderStoreService;
    }

    public List<OrderCountPerStoreDTO> getOrderCount(String orderType) {
        var orderCountStore = getOrderStore(orderType);

        var orders = orderCountStore.all();

        // It is Java 8 function for iterating the elements
        var spliterator = Spliterators.spliteratorUnknownSize(orders, 0);

        return StreamSupport.stream(spliterator, false)
                .map(stringLongKeyValue -> new OrderCountPerStoreDTO(stringLongKeyValue.key, stringLongKeyValue.value))
                .collect(Collectors.toList());

    }

    public OrderCountPerStoreDTO getOrderCountByLocationId(String orderType, String locationId) {

        var orderStore = getOrderStore(orderType);

        var orderCount = orderStore.get(locationId);
        if(orderCount != null){
            return new OrderCountPerStoreDTO(locationId, orderCount);
        }

        return null;

    }

    public List<AllOrdersCountPerStoreDTO> getAllOrders() {

        BiFunction<OrderCountPerStoreDTO, OrderType, AllOrdersCountPerStoreDTO> mapper =
                (orderCountPerStoreDTO, orderType) -> {
                    return new AllOrdersCountPerStoreDTO(orderCountPerStoreDTO.locationId(), orderCountPerStoreDTO.orderCount(),
                            orderType);
                };

        var generalOrderStoreList = getOrderCount(GENERAL_ORDERS)
                .stream()
                .map(orderCountPerStoreDTO -> mapper.apply(orderCountPerStoreDTO, OrderType.GENERAL)).toList();

        var restaurantOrderStoreList = getOrderCount(RESTAURANT_ORDERS)
                .stream()
                .map(orderCountPerStoreDTO -> mapper.apply(orderCountPerStoreDTO, OrderType.RESTAURANT)).toList();


        return Stream.of(generalOrderStoreList, restaurantOrderStoreList).flatMap(Collection::stream).collect(Collectors.toList());

    }

    public OrderRevenueDTO getOrderRevenueByLocationId(String orderType, String locationId) {
        var generalOrderRevenueStore = getOrderRevenueStore(orderType);
        var revenue = generalOrderRevenueStore.get(locationId);
        return new OrderRevenueDTO(locationId, mapOrderType(orderType), revenue);

    }

    public List<OrderRevenueDTO> getOrderRevenue(String orderType) {

        var generalOrderRevenue = getOrderRevenueStore(orderType);
        var revenue = generalOrderRevenue.all();
        var spliterator = Spliterators.spliteratorUnknownSize(revenue, 0);

        return StreamSupport
                .stream(spliterator, false)
                .map(stringTotalRevenueKeyValue -> new OrderRevenueDTO(stringTotalRevenueKeyValue.key,
                        mapOrderType(orderType), stringTotalRevenueKeyValue.value)).collect(Collectors.toList());
    }


    private OrderType mapOrderType(String orderType) {
        return switch (orderType){
            case GENERAL_ORDERS -> OrderType.GENERAL;
            case RESTAURANT_ORDERS -> OrderType.RESTAURANT;
            default -> throw new IllegalStateException("Invalid Order Type");
        };
    }

    private ReadOnlyKeyValueStore<String, Long> getOrderStore(String orderType) {

        return switch (orderType){

            case GENERAL_ORDERS ->  orderStoreService.orderCountStore(GENERAL_ORDERS_COUNT);

            case RESTAURANT_ORDERS -> orderStoreService.orderCountStore(RESTAURANT_ORDERS_COUNT);

            default -> throw new IllegalStateException("Not a valid option");
        };
    }

    public ReadOnlyKeyValueStore<String, TotalRevenue> getOrderRevenueStore(String orderType){
        return switch (orderType){
            case GENERAL_ORDERS ->  orderStoreService.orderRevenueStore(GENERAL_ORDERS_REVENUE);

            case RESTAURANT_ORDERS -> orderStoreService.orderRevenueStore(RESTAURANT_ORDERS_REVENUE);

            default -> throw new IllegalStateException("Not a valid option");
        };
    }
}
