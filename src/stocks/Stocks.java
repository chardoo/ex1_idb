package stocks;

//import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.*;

public class Stocks implements Iterable<StockEntry> {

    private final RandomAccessFile file;

    Stocks(String path) throws FileNotFoundException {
        this.file = new RandomAccessFile(path, "r");
    }

    public StockEntry get(int i) {
        try {
            file.seek(0);
            StockEntry entry = null;
            for (int j = 0; j <= i; j++) {
                ByteBuffer buffer = ByteBuffer.allocate(8);
                file.read(buffer.array());
                long id = buffer.getLong(0);

                buffer = ByteBuffer.allocate(2);
                file.read(buffer.array());
                short nameLength = buffer.getShort(0);

                byte[] nameBytes = new byte[nameLength];
                file.read(nameBytes);

                buffer = ByteBuffer.allocate(16);
                file.read(buffer.array());

                if (j == i) {
                    ByteBuffer fullBuffer = ByteBuffer.allocate(26 + nameLength);
                    fullBuffer.putLong(id);
                    fullBuffer.putShort(nameLength);
                    fullBuffer.put(nameBytes);
                    fullBuffer.put(buffer);
                    fullBuffer.flip();
                    entry = new StockEntry(fullBuffer);
                    break;
                }
            }
            return entry;
        } catch (IOException e) {
            throw new RuntimeException("Error reading file", e);
        }
    }

    @Override
    public Iterator<StockEntry> iterator() {
        return new StockEntryIterator(file);
    }
}


