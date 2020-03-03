import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

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
        Function<Long, Long> adder = new AddThree();
        Long result = adder.apply((long) 4);
        System.out.println("result = " + result);
    }

}
