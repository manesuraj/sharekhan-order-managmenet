package com.myproject.OrderManagment.controller;

import com.myproject.OrderManagment.domain.AllOrdersCountPerStoreDTO;
import com.myproject.OrderManagment.domain.OrderCountPerStoreDTO;
import com.myproject.OrderManagment.domain.OrderRevenueDTO;
import com.myproject.OrderManagment.service.OrderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.util.List;

import static com.myproject.OrderManagment.topology.OrdersTopology.GENERAL_ORDERS;

@RestController
@RequestMapping("/v1/orders")
public class OrderController {

    private OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    // Retrieve Orders Count By Order Type: |

    @GetMapping("/count/{order_type}")
    public ResponseEntity<?> getOrderCount(@PathVariable String order_type,
                                           @RequestParam(value = "location_id", required = false) String locationId){

        try{
            if(StringUtils.hasLength(locationId)){
                OrderCountPerStoreDTO orderCountPerStoreDTO = orderService.getOrderCountByLocationId(order_type, locationId);
                return new ResponseEntity<>(orderCountPerStoreDTO, HttpStatus.OK);
            }else {
                List<OrderCountPerStoreDTO> orderCount =  orderService.getOrderCount(order_type);
                return new ResponseEntity<>(orderCount, HttpStatus.OK);
            }
        }catch (Exception e){
            var problemDetails = ProblemDetail.forStatusAndDetail(HttpStatusCode.valueOf(400), e.getMessage());
            return new ResponseEntity<>(problemDetails, HttpStatus.BAD_REQUEST);
        }
    }

    @GetMapping("/count")
    public ResponseEntity<?> getAllOrders(){
        try{
            List<AllOrdersCountPerStoreDTO> allOrders = orderService.getAllOrders();
            return new ResponseEntity<>(allOrders, HttpStatus.OK);
        }catch (Exception e){
            var problemDetails = ProblemDetail.forStatusAndDetail(HttpStatusCode.valueOf(400), e.getMessage());
            return new ResponseEntity<>(problemDetails, HttpStatus.BAD_REQUEST);
        }
    }

    @GetMapping("/revenue/{order_type}")
    public ResponseEntity<?> getRevenueByOrderType(@PathVariable String order_type,
                                           @RequestParam(value = "location_id", required = false) String locationId){

       try{
           if(StringUtils.hasLength(locationId)){
               OrderRevenueDTO orderRevenueDTO = orderService.getOrderRevenueByLocationId(order_type, locationId);
               return new ResponseEntity<>(orderRevenueDTO, HttpStatus.OK);
           }else {
               List<OrderRevenueDTO> orderRevenueDTOS =  orderService.getOrderRevenue(order_type);
               return new ResponseEntity<>(orderRevenueDTOS, HttpStatus.OK);
           }
       }catch (Exception e){
           var problemDetails = ProblemDetail.forStatusAndDetail(HttpStatusCode.valueOf(400), e.getMessage());
           return new ResponseEntity<>(problemDetails, HttpStatus.BAD_REQUEST);
       }
    }

}
