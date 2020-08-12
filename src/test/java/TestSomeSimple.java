import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.util.Calendar;
import java.util.Date;
import java.util.function.Function;

@Slf4j
public class TestSomeSimple {

    public class AddThree implements Function<Long, Long> {
        @Override
        public Long apply(Long aLong) {
            return null;
        }
    }

    /**
     * Тестирование lambda - функции
     */
    @Test
    public void testLambda() {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.YEAR, -1);
        Date prevYear = cal.getTime();
        System.out.println(prevYear);
        System.out.println(new Date());

        Function<Long, Long> adder = new AddThree();
        Long result = adder.apply((long) 4);
        System.out.println("result = " + result);
    }

}
