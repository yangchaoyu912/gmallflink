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
public class UserBehavior {
    private Long userId;  //加密后的用户ID
    private Long itemId; //加密后的商品ID
    private Integer categoryId; //商品所属类别ID
    private String behavior; //用户行为类型（pv,buy,cart, fav）
    private Long timestamp; //行为发生的时间戳

}
