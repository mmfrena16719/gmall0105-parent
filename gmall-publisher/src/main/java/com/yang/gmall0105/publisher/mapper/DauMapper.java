package com.yang.gmall0105.publisher.mapper;

import java.util.List;
import java.util.Map;

public interface DauMapper {
    public Long selectDauTotal(String date);


    public List<Map> selectDauHourMap(String date);
}
