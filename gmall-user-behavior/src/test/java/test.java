import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

/**
 * @version 1.0
 * @description:
 * @author: 宇文智
 * @date 2020/11/27 16:56
 */
public class test {
    @Test
    public void test() {
        Integer[] a = {2,3,4,51,15};

        ArrayList<Integer> integers = new ArrayList<Integer>(Arrays.asList(a));

        integers.sort(new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                //倒序
                if(o1 > o2){
                    return -1;
                }else if(o1 < o1){
                    return 1;
                }else {
                    return 0;
                }
            }
        });

        System.out.println(integers);
    }
}
