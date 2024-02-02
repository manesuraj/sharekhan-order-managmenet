package com.myproject.OrderManagment.controller;

import com.myproject.OrderManagment.domain.OrderCountPerStoreDTO;
import com.myproject.OrderManagment.domain.OrdersCountPerStoreByWindowsDTO;
import com.myproject.OrderManagment.domain.OrdersRevenuePerStoreByWindowsDTO;
import com.myproject.OrderManagment.service.OrderWindowsService;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.List;

@RestController
@RequestMapping("/v1/orders/windows")
public class OrdersWindowsController {

    private OrderWindowsService orderWindowsService;

    public OrdersWindowsController(OrderWindowsService orderWindowsService) {
        this.orderWindowsService = orderWindowsService;
    }

    @GetMapping("/count/{order_type}")
    public ResponseEntity<?> getOrderCount(@PathVariable String order_type){
        try{
            List<OrdersCountPerStoreByWindowsDTO> orderCount =  orderWindowsService.getOrderCount(order_type);
            return new ResponseEntity<>(orderCount, HttpStatus.OK);
        }catch (Exception e){
            var problemDetails = ProblemDetail.forStatusAndDetail(HttpStatusCode.valueOf(400), e.getMessage());
            return new ResponseEntity<>(problemDetails, HttpStatus.BAD_REQUEST);
        }
    }

    @GetMapping("/count")
    public ResponseEntity<?> getAllOrderCount(
            @RequestParam(value = "from_time", required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime fromDate,
            @RequestParam(value = "from_time", required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime toDate
            ){

        try{
            if(fromDate != null && toDate != null){
                List<OrdersCountPerStoreByWindowsDTO> allOrderCount =
                        orderWindowsService.getAllOrderCountForWindow(fromDate, toDate);
                return new ResponseEntity<>(allOrderCount, HttpStatus.OK);

            }else{
                List<OrdersCountPerStoreByWindowsDTO> allOrderCount =
                        orderWindowsService.getAllOrderCountForWindow();
                return new ResponseEntity<>(allOrderCount, HttpStatus.OK);
            }
        }catch (Exception e){
            var problemDetails = ProblemDetail.forStatusAndDetail(HttpStatusCode.valueOf(400), e
                    .getMessage());
            return new ResponseEntity<>(problemDetails, HttpStatus.BAD_REQUEST);
        }
    }

    @GetMapping("/revenue/{order_type}")
    public ResponseEntity<?> getRevenueByOrderType(@PathVariable String order_type){
        try{
            List<OrdersRevenuePerStoreByWindowsDTO> allOrderRevenue =
                    orderWindowsService.getAllOrderRevenueForWindow(order_type);
            return new ResponseEntity<>(allOrderRevenue, HttpStatus.OK);
        }catch (Exception e){
            var problemDetails = ProblemDetail.forStatusAndDetail(HttpStatusCode.valueOf(400), e
                    .getMessage());
            return new ResponseEntity<>(problemDetails, HttpStatus.BAD_REQUEST);
        }
    }

    @GetMapping("/revenue")
    public ResponseEntity<?> getAllRevenue(
            @RequestParam(value = "from_time", required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime fromDate,
            @RequestParam(value = "from_time", required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime toDate
    ){
        try{
            List<OrdersRevenuePerStoreByWindowsDTO> allOrderRevenue = null;

            if(fromDate != null && toDate != null){
                allOrderRevenue =
                        orderWindowsService.getAllRevenueForWindowWithinTime(fromDate, toDate);
            }else{
                allOrderRevenue =
                        orderWindowsService.getAllRevenueForWindow();
            }

            return new ResponseEntity<>(allOrderRevenue, HttpStatus.OK);
        }catch (Exception e){
            var problemDetails = ProblemDetail.forStatusAndDetail(HttpStatusCode.valueOf(400), e
                    .getMessage());
            return new ResponseEntity<>(problemDetails, HttpStatus.BAD_REQUEST);
        }
    }


}
