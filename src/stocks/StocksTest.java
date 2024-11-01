package stocks;

import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.*;

class StocksTest {

    @Test
    void getTest() {
        try {
            Stocks stocks = new Stocks("stocks.bin");
            StockEntry e1 = stocks.get(0);
            StockEntry e2 = stocks.get(1);
            StockEntry e3 = stocks.get(8);

            assertEquals(new StockEntry(0, "Volvo", 0, 99f), e1);
            assertEquals(new StockEntry(1, "BMW", 0, 99f), e2);
            assertEquals(new StockEntry(8, "Volvo", 2, 98.99010000000001), e3);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void iteratorTest() {
        try {
            Stocks stocks = new Stocks("stocks.bin");
            Iterator<StockEntry> it = stocks.iterator();
            assertTrue(it.hasNext());
            assertEquals(new StockEntry(0, "Volvo", 0, 99f), it.next());
            assertEquals(new StockEntry(1, "BMW", 0, 99f), it.next());
            it.next();
            it.next();
            it.next();
            it.next();
            it.next();
            it.next();
            assertEquals(new StockEntry(8, "Volvo", 2, 98.99010000000001), it.next());
            int count = 9;
            while (it.hasNext()) {
                it.next();
                count++;
            }
            assertEquals(40000, count);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}