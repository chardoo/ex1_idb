package container.impl;

import container.Container;
import util.MetaData;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

public class MapContainer<Value> implements Container<Long, Value> {
	private final Map<Long, Value> data;
	private boolean isOpen = false;
	private long nextKey;
	private final MetaData  metaData;
	public MapContainer( MetaData metaData ) {
		// TODO
		this.data = new HashMap<>();
		this.metaData = metaData;
		this.nextKey = 0;
	}

	@Override
	public MetaData getMetaData() {
		// TODO
		return metaData;
	}

	@Override
	public void open() {
		if(isOpen) return;
		isOpen =true;
	}

	@Override
	public void close() {
		if(!isOpen){return;}
		else {
			isOpen = false;
		}
		// TODO
	}

	@Override
	public Long reserve() throws IllegalStateException {
		// TODO
		if (!isOpen) {throw new IllegalStateException("Container is closed.");}
		else {
			return nextKey++;
		}
	}


	@Override
	public Value get(Long key) throws NoSuchElementException {
		// TODO
		if (!isOpen) {
			throw new IllegalStateException("Container is closed.");
		} else {
			return (Value) data.get(key);
		}
	}

	@Override
	public void update(Long key, Value value) throws NoSuchElementException {
		// TODO
		if (!isOpen) {
			throw new IllegalStateException("Container is closed.");
		} else {
			data.put(key, value);
		}
	}

	@Override
	public void remove(Long key) throws NoSuchElementException {
		// TODO
		if (!isOpen) {throw new IllegalStateException("Container is closed.");}
		else {
			data.remove(key);
		}

	}
}
