package com.yuwenzhi.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @version 1.0
 * @description:
 * @author: 宇文智
 * @date 2020/11/27 15:42
 */


@Data
@AllArgsConstructor
@NoArgsConstructor
public class ItemCount {

   private Long itemId; //商品id

    private Long windowEnd; //该窗口的结束时间

    private Long count; //累计点击次数
}
