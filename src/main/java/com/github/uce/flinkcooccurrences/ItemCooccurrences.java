package com.github.uce.flinkcooccurrences;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Helper class that wraps a {@link it.unimi.dsi.fastutil.ints.IntArrayList} array and serializes it efficiently
 * ignoring elements after the size.
 */
final class ItemCooccurrences implements Iterable<Integer> {

  private int[] reuse = new int[1];

  private int item;
  private int[] otherItems;
  private int size;
  private int k;
  private short increment;

  ItemCooccurrences() {
  }

  void setFields(int item, int otherItem, short increment) {
    reuse[0] = otherItem;
    setFields(item, reuse, 1, -1, increment);
  }

  void setFields(int item, int[] otherItems, int size, short increment) {
    setFields(item, otherItems, size, -1, increment);
  }

  void setFields(int item, int[] otherItems, int size, int k, short increment) {
    this.item = item;
    this.otherItems = otherItems;
    this.size = size;
    this.k = k;
    this.increment = increment;
  }

  public int getItem() {
    return item;
  }

  public short getIncrement() {
    return increment;
  }

  public void clear() {
    this.reuse = null;
    this.item = -1;
    this.otherItems = null;
    this.size = -1;
    this.k = -1;
    this.increment = 0;
  }

  @Override
  public Iterator<Integer> iterator() {
    if (k == -1) {
      return new Iterator<Integer>() {

        int current = 0;

        @Override
        public boolean hasNext() {
          return current < size;
        }

        @Override
        public Integer next() {
          return otherItems[current++];
        }
      };
    } else {
      return new Iterator<Integer>() {

        int current = 0;

        @Override
        public boolean hasNext() {
          return current < (size - 1);
        }

        @Override
        public Integer next() {
          if (hasNext()) {
            if (current == k) {
              current++;
            }
            return otherItems[current];
          } else {
            throw new NoSuchElementException();
          }
        }
      };

    }
  }

  @Override
  public String toString() {
    return "ItemCooccurrences{" +
        "item=" + item +
        ", otherItems=" + Arrays.toString(otherItems) +
        ", increment=" + increment +
        '}';
  }

  public static class Serializer extends com.esotericsoftware.kryo.Serializer<ItemCooccurrences> {

    @Override
    public void write(Kryo kryo, Output output, ItemCooccurrences obj) {
      output.writeInt(obj.item, true);
      output.writeShort(obj.increment);
      if (obj.k == -1) {
        output.writeInt(obj.size, true);
        for (int i = 0; i < obj.size; i++) {
          output.writeInt(obj.otherItems[i], true);
        }
      } else {
        output.writeInt(obj.size - 1, true);
        for (int i = 0; i < obj.size; i++) {
          if (i != obj.k) {
            output.writeInt(obj.otherItems[i], true);
          }
        }
      }
    }

    @Override
    public ItemCooccurrences read(Kryo kryo, Input input, Class<ItemCooccurrences> type) {
      int item = input.readInt(true);
      short increment = input.readShort();
      int size = input.readInt(true);
      int[] otherItems = new int[size];
      for (int i = 0; i < size; i++) {
        otherItems[i] = input.readInt(true);
      }
      ItemCooccurrences itemCooccurrences = new ItemCooccurrences();
      itemCooccurrences.setFields(item, otherItems, size, -1, increment);
      return itemCooccurrences;
    }
  }

}
