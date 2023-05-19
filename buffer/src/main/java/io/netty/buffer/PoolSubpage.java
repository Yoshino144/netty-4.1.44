/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.netty.buffer;

final class PoolSubpage<T> implements PoolSubpageMetric {

    // 记录当前PoolSubpage的8KB内存块是从哪一个PoolChunk中申请到的
    final PoolChunk<T> chunk;
    // 当前PoolSubpage申请的8KB内存在PoolChunk中memoryMap中的下标索引
    private final int memoryMapIdx;
    // 当前PoolSubpage占用的8KB内存在PoolChunk中相对于叶节点的起始点的偏移量
    private final int runOffset;
    // 当前PoolSubpage的页大小，默认为8KB
    private final int pageSize;
    // 存储当前PoolSubpage中各个内存块的使用情况
    private final long[] bitmap;

    PoolSubpage<T> prev;	// 指向前置节点的指针
    PoolSubpage<T> next;	// 指向后置节点的指针

    boolean doNotDestroy;	// 表征当前PoolSubpage是否已经被销毁了
    int elemSize;	// 表征每个内存块的大小，比如我们这里的就是16
    private int maxNumElems;	// 记录内存块的总个数
    private int bitmapLength;	// 记录总共可使用的bitmap数组的元素的个数
    // 记录下一个可用的节点，初始为0，只要在该PoolSubpage中申请过一次内存，就会更新为-1，
    // 然后一直不会发生变化
    private int nextAvail;
    // 剩余可用的内存块的个数
    private int numAvail;

    // TODO: Test if adding padding helps under contention
    //private long pad0, pad1, pad2, pad3, pad4, pad5, pad6, pad7;

    /** 创建链表头的特殊构造函数 */
    PoolSubpage(int pageSize) {
        chunk = null;
        memoryMapIdx = -1;
        runOffset = -1;
        elemSize = -1;
        this.pageSize = pageSize;
        bitmap = null;
    }

    PoolSubpage(PoolSubpage<T> head, PoolChunk<T> chunk, int memoryMapIdx, int runOffset, int pageSize, int elemSize) {
        this.chunk = chunk;
        this.memoryMapIdx = memoryMapIdx;
        this.runOffset = runOffset;
        this.pageSize = pageSize;	// 初始化当前PoolSubpage总内存大小，默认为8KB
        // 计算bitmap长度，这里pageSize >>> 10其实就是将pageSize / 1024，得到的是8，
        // 从这里就可以看出，无论内存块的大小是多少，这里的bitmap长度永远是8，因为pageSize始终是不变的
        bitmap = new long[pageSize >>> 10];
        // 对其余的属性进行初始化
        init(head, elemSize);
    }

    void init(PoolSubpage<T> head, int elemSize) {
        doNotDestroy = true;
        // elemSize记录了当前内存块的大小
        this.elemSize = elemSize;
        if (elemSize != 0) {
            // 初始时，numAvail记录了可使用的内存块个数，其个数可以通过pageSize / elemSize计算，
            // 我们这里就是8192 / 16 = 512。maxNumElems指的是最大可使用的内存块个数，
            // 初始时其是与可用内存块个数一致的。
            maxNumElems = numAvail = pageSize / elemSize;
            nextAvail = 0;// 初始时，nextAvail是0
            // 这里bitmapLength记录了可以使用的bitmap的元素个数，这是因为，我们示例使用的内存块大小是16，
            // 因而其总共有512个内存块，需要8个long才能记录，但是对于一些大小更大的内存块，比如smallSubpagePools
            // 中内存块为1024字节大小，那么其只有8192 / 1024 = 8个内存块，也就只需要一个long就可以表示，
            // 此时bitmapLength就是8。
            // 这里的计算方式应该是bitmapLength = maxNumElems / 64，因为64是一个long的总字节数，
            // 但是Netty将其进行了优化，也就是这里的maxNumElems >>> 6，这是因为2的6次方正好为64
            bitmapLength = maxNumElems >>> 6;
            // 这里(maxNumElems & 63) != 0就是判断元素个数是否小于64，如果小于，则需要将bitmapLegth加一。
            // 这是因为如果其小于64，前面一步的位移操作结果为0，但其还是需要一个long来记录
            if ((maxNumElems & 63) != 0) {
                bitmapLength ++;
            }

            // 对bitmap数组的值进行初始化
            for (int i = 0; i < bitmapLength; i ++) {
                bitmap[i] = 0;
            }
        }

        // 将当前PoolSubpage添加到PoolSubpage的链表中，也就是最开始图中的链表
        addToPool(head);
    }

    /**
     * Returns the bitmap index of the subpage allocation.
     * 返回子页面分配的位图索引。
     */
    long allocate() {
        if (elemSize == 0) {
            return toHandle(0);
        }

        // 如果当前PoolSubpage没有可用的元素，或者已经被销毁了，则返回-1
        if (numAvail == 0 || !doNotDestroy) {
            return -1;
        }

        // 计算下一个可用的内存块的位置
        final int bitmapIdx = getNextAvail();
        int q = bitmapIdx >>> 6; // 获取该内存块是bitmap数组中的第几号元素
        int r = bitmapIdx & 63; // 获取该内存块是bitmap数组中q号位元素的第多少位
        assert (bitmap[q] >>> r & 1) == 0;
        bitmap[q] |= 1L << r; 	// 将bitmap数组中q号元素的目标内存块位置标记为1，表示已经使用

        // 如果当前PoolSubpage中可用的内存块为0，则将其从链表中移除
        if (-- numAvail == 0) {
            removeFromPool();
        }

        // 将得到的bitmapIdx放到返回值的高32位中
        return toHandle(bitmapIdx);
    }

    /**
     * @return {@code true} if this subpage is in use.
     *         {@code false} if this subpage is not used by its chunk and thus it's OK to be released.
     * 如果这个子页面的区块没有使用，因此可以发布。
     */
    boolean free(PoolSubpage<T> head, int bitmapIdx) {
        if (elemSize == 0) {
            return true;
        }
        // 获取当前需要释放的内存块是在bitmap中的第几号元素
        int q = bitmapIdx >>> 6;
        // 获取当前释放的内存块是在q号元素的long型数的第几位
        int r = bitmapIdx & 63;
        // 将目标位置标记为0，表示可使用状态
        bitmap[q] ^= 1L << r;

        // 设置下一个可使用的数据
        setNextAvail(bitmapIdx);

        // numAvail如果等于0，表示之前已经被移除链表了，因而这里释放后需要将其添加到链表中
        if (numAvail++ == 0) {
            addToPool(head);
            return true;
        }

        // 如果可用的数量小于最大数量，则表示其还是在链表中，因而直接返回true
        if (numAvail != maxNumElems) {
            return true;
        } else {
            // Subpage 未使用 (numAvail == maxNumElems)
            if (prev == next) {
                // 如果此子页面是池中唯一剩下的子页面，请不要删除。
                return true;
            }

            // Remove this subpage from the pool if there are other subpages left in the pool.
            // 如果池中还有其他子页，请从池中删除此子页。
            doNotDestroy = false;
            removeFromPool();
            return false;
        }
    }

    private void addToPool(PoolSubpage<T> head) {
        assert prev == null && next == null;
        prev = head;
        next = head.next;
        next.prev = this;
        head.next = this;
    }

    private void removeFromPool() {
        assert prev != null && next != null;
        prev.next = next;
        next.prev = prev;
        next = null;
        prev = null;
    }

    private void setNextAvail(int bitmapIdx) {
        nextAvail = bitmapIdx;
    }

    private int getNextAvail() {
        int nextAvail = this.nextAvail;
        // 如果是第一次尝试获取数据，则直接返回bitmap第0号位置的long的第0号元素，
        // 这里nextAvail初始时为0，在第一次申请之后就会变为-1，后面将不再发生变化，
        // 通过该变量可以判断是否是第一次尝试申请内存
        if (nextAvail >= 0) {
            this.nextAvail = -1;
            return nextAvail;
        }

        // 如果不是第一次申请内存，则在bitmap中进行遍历获取
        return findNextAvail();
    }

    private int findNextAvail() {
        final long[] bitmap = this.bitmap;
        final int bitmapLength = this.bitmapLength;
        // 这里的基本思路就是对bitmap数组进行遍历，首先判断其是否有未使用的内存是否全部被使用过
        // 如果有未被使用的内存，那么就在该元素中找可用的内存块的位置
        for (int i = 0; i < bitmapLength; i++) {
            long bits = bitmap[i];
            if (~bits != 0) {	// 判断当前long型元素中是否有可用内存块
                return findNextAvail0(i, bits);
            }
        }
        return -1;
    }

    // 入参中i表示当前是bitmap数组中的第几个元素，bits表示该元素的值
    private int findNextAvail0(int i, long bits) {
        final int maxNumElems = this.maxNumElems;
        final int baseVal = i << 6;	// 这里baseVal就是将当前是第几号元素放到返回值的第7~9号位置上

        // 对bits的0~63号位置进行遍历，判断其是否为0，为0表示该位置是可用内存块，从而将位置数据
        // 和baseVal进行或操作，从而得到一个表征目标内存块位置的整型数据
        for (int j = 0; j < 64; j++) {
            if ((bits & 1) == 0) {	// 判断当前位置是否为0，如果为0，则表示是目标内存块
                int val = baseVal | j;	// 将内存快的位置数据和其位置j进行或操作，从而得到返回值
                if (val < maxNumElems) {
                    return val;
                } else {
                    break;
                }
            }
            bits >>>= 1;	// 将bits不断的向右移位，以找到第一个为0的位置
        }
        return -1;
    }

    private long toHandle(int bitmapIdx) {
        return 0x4000000000000000L | (long) bitmapIdx << 32 | memoryMapIdx;
    }

    @Override
    public String toString() {
        final boolean doNotDestroy;
        final int maxNumElems;
        final int numAvail;
        final int elemSize;
        if (chunk == null) {
            // This is the head so there is no need to synchronize at all as these never change.
            doNotDestroy = true;
            maxNumElems = 0;
            numAvail = 0;
            elemSize = -1;
        } else {
            synchronized (chunk.arena) {
                if (!this.doNotDestroy) {
                    doNotDestroy = false;
                    // Not used for creating the String.
                    maxNumElems = numAvail = elemSize = -1;
                } else {
                    doNotDestroy = true;
                    maxNumElems = this.maxNumElems;
                    numAvail = this.numAvail;
                    elemSize = this.elemSize;
                }
            }
        }

        if (!doNotDestroy) {
            return "(" + memoryMapIdx + ": not in use)";
        }

        return "(" + memoryMapIdx + ": " + (maxNumElems - numAvail) + '/' + maxNumElems +
                ", offset: " + runOffset + ", length: " + pageSize + ", elemSize: " + elemSize + ')';
    }

    @Override
    public int maxNumElements() {
        if (chunk == null) {
            // It's the head.
            return 0;
        }

        synchronized (chunk.arena) {
            return maxNumElems;
        }
    }

    @Override
    public int numAvailable() {
        if (chunk == null) {
            // It's the head.
            return 0;
        }

        synchronized (chunk.arena) {
            return numAvail;
        }
    }

    @Override
    public int elementSize() {
        if (chunk == null) {
            // It's the head.
            return -1;
        }

        synchronized (chunk.arena) {
            return elemSize;
        }
    }

    @Override
    public int pageSize() {
        return pageSize;
    }

    void destroy() {
        if (chunk != null) {
            chunk.destroy();
        }
    }
}
