package com.yuwenzhi.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PvCount {

    private String pv;
    private Long windowEnd;
    private Long count;

}
