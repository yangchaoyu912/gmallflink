package com.yuwenzhi.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
//存储一个小时内网站的访客量
public class UvCount {
    private String uv;
    private String windowEnd;
    private Long count;
}
