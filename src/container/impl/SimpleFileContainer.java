package container.impl;

import container.Container;
import io.FixedSizeSerializer;
import io.Serializer;
import util.MetaData;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

public class SimpleFileContainer<Value> implements Container<Long, Value> {

	private final Path dataFile;
	private final Path metaFile;
	private final FixedSizeSerializer<Value> serializer;
	private boolean isOpen = false;
	private long nextKey;
	private final Set<Long> deletedKeys = new HashSet<>();

	public SimpleFileContainer(Path directory, String filenamePrefix, FixedSizeSerializer<Value> serializer)    {
		// TODO
		this.dataFile = directory.resolve(filenamePrefix + "_data.bin");
		this.metaFile = directory.resolve(filenamePrefix + "_meta.bin");
		this.serializer = serializer;

	}


	@Override
	public MetaData getMetaData() {
		// TODO
		try (RandomAccessFile raf = new RandomAccessFile(String.valueOf(metaFile), "r")) {
			byte flag = raf.readByte();
			if (flag == 0) {
				throw new NoSuchElementException("Key not found");
			}
			byte[] data = new byte[serializer.getSerializedSize()];
			raf.readFully(data);
			return (MetaData) serializer.deserialize(ByteBuffer.wrap(data));
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void open() {
		// TODO
		if(isOpen){
			return;
		}
		else {
			if (!Files.exists(dataFile)) {
                try {
                    Files.createFile(dataFile);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
			if (!Files.exists(metaFile)) {
                try {
                    Files.createFile(metaFile);
					Files.write(metaFile, ByteBuffer.allocate(Long.BYTES).putLong(0).array()); // Initial nextKey = 0

                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

			} else {
                byte[] keyBytes = null;
                try {
                    keyBytes = Files.readAllBytes(metaFile);
					this.nextKey = ByteBuffer.wrap(keyBytes).getLong();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

			}
			isOpen = true;
		}
	}

	@Override
	public void close() {
		// TODO
		if(!isOpen) return;
		isOpen = false;

	}

	@Override
	public Long reserve() throws IllegalStateException {
		// TODO
		if(!isOpen) return null;
		return nextKey++;
	}

	@Override
	public void update(Long key, Value value) throws NoSuchElementException {
		// TODO
		if(!isOpen) {
			throw new NoSuchElementException();
		}
		try {
			if(deletedKeys.contains(key))
			{
				return;
			}
			else {
				writeToDataFile(key, value);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	}


	@Override
	public Value get(Long key) throws NoSuchElementException {
		// TODO
		if(!isOpen) {
			throw new NoSuchElementException();
		}
        try {
			if(deletedKeys.contains(key))
			{
				return null;
			}
            return readFromDataFile(key);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

	@Override
	public void remove(Long key) throws NoSuchElementException {
		// TODO
		if(!isOpen) {
			throw new NoSuchElementException();
		}
		deletedKeys.add(key);

	}


	private void writeToDataFile(Long key, Value value) throws IOException {
		try (RandomAccessFile raf = new RandomAccessFile(String.valueOf(dataFile), "rw")) {
			byte[] buffer = new byte[serializer.getSerializedSize(value)]; // Allocate buffer of fixed size
			serializer.serialize(value, ByteBuffer.wrap(buffer)); // Serialize value into buffer
			raf.seek(key * (serializer.getSerializedSize(value) + 1)); // Move to the appropriate position in the file
			raf.writeByte(1); // Write a flag to indicate the entry is active
			raf.write(buffer); // Write serialized data from buffer
		}
	}

	private Value readFromDataFile(Long key) throws IOException, NoSuchElementException {
		try (RandomAccessFile raf = new RandomAccessFile(String.valueOf(dataFile), "r")) {
			raf.seek(key * serializer.getSerializedSize());
			byte flag = raf.readByte();
			if (flag == 0) {
				throw new NoSuchElementException("Key not found");
			}
			byte[] data = new byte[serializer.getSerializedSize()];
			raf.readFully(data);
			return serializer.deserialize(ByteBuffer.wrap(data));
		}
	}
}
