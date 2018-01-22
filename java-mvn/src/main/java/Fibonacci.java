/**
 * Created by Seven7 on 2018/1/22.
 */
public class Fibonacci {
    public static int fibonacci(int n) {
        if (n < 0) return n;

        if (n == 0 || n == 1) {
            return 1;
        }

        int fn_2 = 1, fn_1 = fn_2;

        for (int i = 2; i < n; i++) {
            int temp = fn_1;

            fn_1 = fn_2 + fn_1;

            fn_2 = temp;
        }

        return fn_1 + fn_2;
    }

}
