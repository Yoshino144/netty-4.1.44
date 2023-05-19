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

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * 从PoolChunk分配PageRunPoolSubpage的算法描述
 *
 * 注释：以下术语对于理解代码很重要
 * > page  - 页面是可以分配的最小内存块单元
 * > chunk - 区块是页面的集合
 * > 在这段代码中，chunkSize = 2^{maxOrder} * pageSize * pageSize
 *
 * 首先，我们分配一个大小=chunkSize的字节数组，当需要创建一个给定大小的ByteBuf时，
 * 我们在字节数组中寻找有足够空位的第一个位置来容纳要求的大小，并返回一个编码这个偏移信息的（长）句柄。
 * 返回一个编码这个偏移信息的（长）句柄，（这个内存段然后被标记为保留，所以它总是被使用。
 * 这个内存段被标记为保留，所以它总是被一个ByteBuf使用，而不是更多
 *
 * 为了简单起见，所有的大小都根据PoolArena#normalizeCapacity方法进行了标准化处理。
 * 这可以确保当我们请求的内存段大小>=pageSize时，规范化的Capacity 等于2的下一个最接近的幂
 *
 * 为了在大块中搜索第一个偏移量，而这个偏移量至少有要求的大小，我们构建一个
 * 完整的平衡二叉树并将其存储在一个数组中（就像堆一样）--memoryMap
 *
 * 这棵树看起来像这样（括号里提到了每个节点的大小）。
 *
 * depth=0        1 node (chunkSize)
 * depth=1        2 nodes (chunkSize/2)
 * ..
 * ..
 * depth=d        2^d nodes (chunkSize/2^d)
 * ..
 * depth=maxOrder 2^maxOrder nodes (chunkSize/2^{maxOrder} = pageSize)
 *
 * depth=maxOrder 是最后一层，叶子由页面组成
 *
 * 有了这个树，在chunkArray中的搜索就可以这样进行：
 * 为了分配一个大小为chunkSize/2^k的内存段，我们搜索高度为k的第一个节点（从左边开始）。
 * 的第一个节点，该节点是未使用的
 *
 * Algorithm:
 * ----------
 * Encode the tree in memoryMap with the notation
 *   memoryMap[id] = x => in the subtree rooted at id, the first node that is free to be allocated
 *   is at depth x (counted from depth=0) i.e., at depths [depth_of_id, x), there is no node that is free
 *
 *  As we allocate & free nodes, we update values stored in memoryMap so that the property is maintained
 *
 * Initialization -
 *   In the beginning we construct the memoryMap array by storing the depth of a node at each node
 *     i.e., memoryMap[id] = depth_of_id
 *
 * Observations:
 * -------------
 * 1) memoryMap[id] = depth_of_id  => it is free / unallocated
 * 2) memoryMap[id] > depth_of_id  => at least one of its child nodes is allocated, so we cannot allocate it, but
 *                                    some of its children can still be allocated based on their availability
 * 3) memoryMap[id] = maxOrder + 1 => the node is fully allocated & thus none of its children can be allocated, it
 *                                    is thus marked as unusable
 *
 * Algorithm: [allocateNode(d) => we want to find the first node (from left) at height h that can be allocated]
 * ----------
 * 1) start at root (i.e., depth = 0 or id = 1)
 * 2) if memoryMap[1] > d => cannot be allocated from this chunk
 * 3) if left node value <= h; we can allocate from left subtree so move to left and repeat until found
 * 4) else try in right subtree
 *
 * Algorithm: [allocateRun(size)]
 * ----------
 * 1) Compute d = log_2(chunkSize/size)
 * 2) Return allocateNode(d)
 *
 * Algorithm: [allocateSubpage(size)]
 * ----------
 * 1) use allocateNode(maxOrder) to find an empty (i.e., unused) leaf (i.e., page)
 * 2) use this handle to construct the PoolSubpage object or if it already exists just call init(normCapacity)
 *    note that this PoolSubpage object is added to subpagesPool in the PoolArena when we init() it
 *
 * Note:
 * -----
 * In the implementation for improving cache coherence,
 * we store 2 pieces of information depth_of_id and x as two byte values in memoryMap and depthMap respectively
 *
 * memoryMap[id]= depth_of_id  is defined above
 * depthMap[id]= x  indicates that the first node which is free to be allocated is at depth x (from root)
 */
final class PoolChunk<T> implements PoolChunkMetric {

    private static final int INTEGER_SIZE_MINUS_ONE = Integer.SIZE - 1;

    // 标识当前Chunk属于哪个Arena
    final PoolArena<T> arena;
    // 实际16MB内存块，如果是Direct则为JDKByteBuffer，如果是Heap则为byte数组
    final T memory;
    // 忽略 内存对齐 认为是0
    final int offset;
    // 树，初始值与depthMap一样
    private final byte[] memoryMap;
    // 节点 - 节点深度
    private final byte[] depthMap;
    // 分配Subpage集合（如果Chunk没有分配过小于等于4KB的内存，这里不会有Subpage实例）
    private final PoolSubpage<T>[] subpages;
    // -8192 掩码，用于判断分配内存是否大于8k，x&-8192 != 0 代表超过8k
    private final int subpageOverflowMask;
    // 8192 页大小
    private final int pageSize;
    // log2(页大小) = 13
    private final int pageShifts;
    // 树深度 = 11
    private final int maxOrder;
    // chunk大小 16MB
    private final int chunkSize;
    // log2(chunk大小) = 24
    private final int log2ChunkSize;
    // Subpage数组大小 = 2048 = 16MB/8KB
    private final int maxSubpageAllocs;
    // 标记位 = 树深度 + 1 = 11 + 1 = 12
    private final byte unusable;
    // 缓存ByteBuffer，减少New对象和GC
    private final Deque<ByteBuffer> cachedNioBuffers;
    // 剩余可分配字节数 = 16MB - 已分配字节数
    int freeBytes;
    // 表示目前在哪个PoolChunkList中
    PoolChunkList<T> parent;
    // 前驱节点
    PoolChunk<T> prev;
    // 后驱节点
    PoolChunk<T> next;

    final boolean unpooled;


    // TODO: Test if adding padding helps under contention
    //private long pad0, pad1, pad2, pad3, pad4, pad5, pad6, pad7;

    PoolChunk(PoolArena<T> arena, T memory, int pageSize, int maxOrder, int pageShifts, int chunkSize, int offset) {
        unpooled = false;
        this.arena = arena;
        this.memory = memory;
        this.pageSize = pageSize;
        this.pageShifts = pageShifts;
        this.maxOrder = maxOrder;
        this.chunkSize = chunkSize;
        this.offset = offset;
        unusable = (byte) (maxOrder + 1);
        log2ChunkSize = log2(chunkSize);
        subpageOverflowMask = ~(pageSize - 1);
        freeBytes = chunkSize;

        assert maxOrder < 30 : "maxOrder should be < 30, but is: " + maxOrder;
        maxSubpageAllocs = 1 << maxOrder;

        // 创建树
        memoryMap = new byte[maxSubpageAllocs << 1];
        depthMap = new byte[memoryMap.length];
        int memoryMapIndex = 1;
        for (int d = 0; d <= maxOrder; ++ d) { // 一次向下移动一级树
            int depth = 1 << d;
            for (int p = 0; p < depth; ++ p) {
                // 在每个级别中，从左到右遍历并将值设置为子树的深度
                memoryMap[memoryMapIndex] = (byte) d;
                depthMap[memoryMapIndex] = (byte) d;
                memoryMapIndex ++;
            }
        }
        // 一个2048个元素的空数组
        subpages = newSubpageArray(maxSubpageAllocs);
        cachedNioBuffers = new ArrayDeque<ByteBuffer>(8);
    }

    /** Creates a special chunk that is not pooled. */
    PoolChunk(PoolArena<T> arena, T memory, int size, int offset) {
        unpooled = true;
        this.arena = arena;
        this.memory = memory;
        this.offset = offset;
        memoryMap = null;
        depthMap = null;
        subpages = null;
        subpageOverflowMask = 0;
        pageSize = 0;
        pageShifts = 0;
        maxOrder = 0;
        unusable = (byte) (maxOrder + 1);
        chunkSize = size;
        log2ChunkSize = log2(chunkSize);
        maxSubpageAllocs = 0;
        cachedNioBuffers = null;
    }

    @SuppressWarnings("unchecked")
    private PoolSubpage<T>[] newSubpageArray(int size) {
        return new PoolSubpage[size];
    }

    @Override
    public int usage() {
        final int freeBytes;
        synchronized (arena) {
            freeBytes = this.freeBytes;
        }
        return usage(freeBytes);
    }

    private int usage(int freeBytes) {
        if (freeBytes == 0) {
            return 100;
        }

        int freePercentage = (int) (freeBytes * 100L / chunkSize);
        if (freePercentage == 0) {
            return 99;
        }
        return 100 - freePercentage;
    }

    boolean allocate(PooledByteBuf<T> buf, int reqCapacity, int normCapacity) {
        final long handle;
        if ((normCapacity & subpageOverflowMask) != 0) { // >= pageSize即Normal请求
            handle =  allocateRun(normCapacity);
        } else {
            // Tiny和Small请求
            handle = allocateSubpage(normCapacity);
        }

        if (handle < 0) {
            return false;
        }
        ByteBuffer nioBuffer = cachedNioBuffers != null ? cachedNioBuffers.pollLast() : null;
        initBuf(buf, nioBuffer, handle, reqCapacity);
        return true;
    }

    /**
     * Update method used by allocate
     * This is triggered only when a successor is allocated and all its predecessors
     * need to update their state
     * The minimal depth at which subtree rooted at id has some free space
     *
     * allocate使用的Update方法只有当一个后继被分配并且它的所有前置都需要更新它们的状态时，
     * 才会触发该方法。根在id处的子树具有一些可用空间的最小深度
     *
     * @param id id
     */
    private void updateParentsAlloc(int id) {
        while (id > 1) {
            int parentId = id >>> 1;
            byte val1 = value(id); // 父节点值
            byte val2 = value(id ^ 1); // 父节点的兄弟（左或者右）节点值
            byte val = val1 < val2 ? val1 : val2; // 取较小值
            setValue(parentId, val);
            id = parentId; // 递归更新
        }
    }

    /**
     * Update method used by free
     * This needs to handle the special case when both children are completely free
     * in which case parent be directly allocated on request of size = child-size * 2
     *
     * free使用的更新方法当两个子项都完全空闲时，需要处理特殊情况，
     * 在这种情况下，根据size＝child - size * 2的请求直接分配父项
     *
     * @param id id
     */
    private void updateParentsFree(int id) {
        int logChild = depth(id) + 1;
        while (id > 1) {
            int parentId = id >>> 1;
            byte val1 = value(id);
            byte val2 = value(id ^ 1);
            // 在第一次迭代中等于log，随后在遍历时从logChild中减少1
            logChild -= 1; // in first iteration equals log, subsequently reduce 1 from logChild as we traverse up

            if (val1 == logChild && val2 == logChild) {
                // 此时子节点均空闲，父节点值高度-1
                setValue(parentId, (byte) (logChild - 1));
            } else {
                // // 此时至少有一个子节点被分配，取最小值
                byte val = val1 < val2 ? val1 : val2;
                setValue(parentId, val);
            }

            id = parentId;
        }
    }

    /**
     * Algorithm to allocate an index in memoryMap when we query for a free node
     * at depth d
     * 当我们在深度d处查询空闲节点时，在memoryMap中分配索引的算法
     *
     * @param d depth
     * @return index in memoryMap
     */
    private int allocateNode(int d) {
        int id = 1;
        // 所有高度<d 的节点 id & initial = 0
        int initial = - (1 << d);
        byte val = value(id); // = memoryMap[id]
        if (val > d) { // 没有满足需求的节点
            return -1;
        }

        // val<d 子节点可满足需求
        // id & initial == 0 高度<d
        while (val < d || (id & initial) == 0) {
            id <<= 1;   // 高度加1，进入子节点
            val = value(id); // = memoryMap[id]
            if (val > d) { // 左节点不满足
                id ^= 1; // 右节点
                val = value(id);
            }
        }

        byte value = value(id);
        assert value == d && (id & initial) == 1 << d : String.format("val = %d, id & initial = %d, d = %d",
                value, id & initial, d);

        // 此时val = d
        setValue(id, unusable); // 找到符合需求的节点并标记为不可用
        updateParentsAlloc(id);  // 更新祖先节点的分配信息
        return id;
    }

    /**
     * 分配页面 run  (>=1)
     *
     * @param normCapacity normalized capacity
     * @return index in memoryMap
     */
    private long allocateRun(int normCapacity) {
        // 计算满足需求的节点的高度
        int d = maxOrder - (log2(normCapacity) - pageShifts);
        // 在该高度层找到空闲的节点
        int id = allocateNode(d);
        if (id < 0) {
            return id;// 没有找到
        }
        freeBytes -= runLength(id); // 分配后剩余的字节数
        return id;
    }

    /**
     * Create / initialize a new PoolSubpage of normCapacity
     * Any PoolSubpage created / initialized here is added to subpage pool in the PoolArena that owns this PoolChunk
     *
     * 创建初始化一个标准容量的新 PoolSubpage 在此初始化的任何创建的
     * PoolSubpage 都将添加到拥有此 PoolChunk 的 PoolArena 中的子页面池
     *
     * @param normCapacity normalized capacity
     * @return index in memoryMap
     */
    private long allocateSubpage(int normCapacity) {
        // Obtain the head of the PoolSubPage pool that is owned by the PoolArena and synchronize on it.
        // 获取PoolArena拥有的PoolSubPage池的头，并在其上进行同步。
        // This is need as we may add it back and so alter the linked-list structure.
        // This is need as we may add it back and so alter the linked-list structure.
        PoolSubpage<T> head = arena.findSubpagePoolHead(normCapacity);
        int d = maxOrder; // subpages are only be allocated from pages i.e.,
        // leaves 子页面只能从页面中分配，即叶子
        // 加锁，分配过程会修改链表结构
        synchronized (head) {
            int id = allocateNode(d);
            if (id < 0) {
                return id; // 叶子节点全部分配完毕
            }

            final PoolSubpage<T>[] subpages = this.subpages;
            final int pageSize = this.pageSize;

            freeBytes -= pageSize;

            // 得到叶子节点的偏移索引，从0开始，即2048-0,2049-1,...
            int subpageIdx = subpageIdx(id);
            PoolSubpage<T> subpage = subpages[subpageIdx];
            if (subpage == null) {
                subpage = new PoolSubpage<T>(head, this, id, runOffset(id), pageSize, normCapacity);
                subpages[subpageIdx] = subpage;
            } else {
                subpage.init(head, normCapacity);
            }
            return subpage.allocate();
        }
    }

    /**
     * Free a subpage or a run of pages
     * When a subpage is freed from PoolSubpage, it might be added back to subpage pool of the owning PoolArena
     * If the subpage pool in PoolArena has at least one other PoolSubpage of given elemSize, we can
     * completely free the owning Page so it is available for subsequent allocations
     *
     * 释放一个子页面或一系列页面当一个子页面从PoolSubpage中释放时，
     * 它可能会被添加回所属PoolArena的子页面池。
     * 如果PoolArena中的子页面库至少有一个其他给定elemSize的PoolSubpage，
     * 我们可以完全释放所属页面，以便后续分配
     *
     * @param handle handle to free
     */
    void free(long handle, ByteBuffer nioBuffer) {
        int memoryMapIdx = memoryMapIdx(handle);
        int bitmapIdx = bitmapIdx(handle);

        if (bitmapIdx != 0) { // free a subpage  // 需要释放subpage
            PoolSubpage<T> subpage = subpages[subpageIdx(memoryMapIdx)];
            assert subpage != null && subpage.doNotDestroy;

            // Obtain the head of the PoolSubPage pool that is owned by the PoolArena and synchronize on it.
            // 获取PoolArena拥有的PoolSubPage池的头，并在其上进行同步。
            // This is need as we may add it back and so alter the linked-list structure.
            // 这是必要的，因为我们可能会将其添加回来，从而改变链表结构。
            PoolSubpage<T> head = arena.findSubpagePoolHead(subpage.elemSize);
            synchronized (head) {
                if (subpage.free(head, bitmapIdx & 0x3FFFFFFF)) {
                    return; // 此时释放了subpage中的一部分内存（即请求的）
                }
            }
        }
        freeBytes += runLength(memoryMapIdx);
        setValue(memoryMapIdx, depth(memoryMapIdx));  // 节点分配信息还原为高度值
        updateParentsFree(memoryMapIdx); // 更新祖先节点的分配信息

        if (nioBuffer != null && cachedNioBuffers != null &&
                cachedNioBuffers.size() < PooledByteBufAllocator.DEFAULT_MAX_CACHED_BYTEBUFFERS_PER_CHUNK) {
            cachedNioBuffers.offer(nioBuffer);
        }
    }

    void initBuf(PooledByteBuf<T> buf, ByteBuffer nioBuffer, long handle, int reqCapacity) {
        int memoryMapIdx = memoryMapIdx(handle);
        int bitmapIdx = bitmapIdx(handle);
        if (bitmapIdx == 0) {
            byte val = value(memoryMapIdx);
            assert val == unusable : String.valueOf(val);
            buf.init(this, nioBuffer, handle, runOffset(memoryMapIdx) + offset,
                    reqCapacity, runLength(memoryMapIdx), arena.parent.threadCache());
        } else {
            initBufWithSubpage(buf, nioBuffer, handle, bitmapIdx, reqCapacity);
        }
    }

    void initBufWithSubpage(PooledByteBuf<T> buf, ByteBuffer nioBuffer, long handle, int reqCapacity) {
        initBufWithSubpage(buf, nioBuffer, handle, bitmapIdx(handle), reqCapacity);
    }

    private void initBufWithSubpage(PooledByteBuf<T> buf, ByteBuffer nioBuffer,
                                    long handle, int bitmapIdx, int reqCapacity) {
        assert bitmapIdx != 0;

        int memoryMapIdx = memoryMapIdx(handle);

        PoolSubpage<T> subpage = subpages[subpageIdx(memoryMapIdx)];
        assert subpage.doNotDestroy;
        assert reqCapacity <= subpage.elemSize;

        buf.init(
            this, nioBuffer, handle,
            runOffset(memoryMapIdx) + (bitmapIdx & 0x3FFFFFFF) * subpage.elemSize + offset,
                reqCapacity, subpage.elemSize, arena.parent.threadCache());
    }

    private byte value(int id) {
        return memoryMap[id];
    }

    private void setValue(int id, byte val) {
        memoryMap[id] = val;
    }

    private byte depth(int id) {
        return depthMap[id];
    }

    private static int log2(int val) {
        // compute the (0-based, with lsb = 0) position of highest set bit i.e, log2
        return INTEGER_SIZE_MINUS_ONE - Integer.numberOfLeadingZeros(val);
    }

    // 得到节点对应可分配的字节，1号节点为16MB-ChunkSize，2048节点为8KB-PageSize
    private int runLength(int id) {
        // represents the size in #bytes supported by node 'id' in the tree
        return 1 << log2ChunkSize - depth(id);
    }

    // 得到节点在chunk底层的字节数组中的偏移量
    // 2048-0, 2049-8K，2050-16K
    private int runOffset(int id) {
        // represents the 0-based offset in #bytes from start of the byte-array chunk
        int shift = id ^ 1 << depth(id);
        return shift * runLength(id);
    }

    // 得到第11层节点的偏移索引，= id - 2048
    private int subpageIdx(int memoryMapIdx) {
        return memoryMapIdx ^ maxSubpageAllocs; // remove highest set bit, to get offset
    }

    private static int memoryMapIdx(long handle) {
        return (int) handle;
    }

    private static int bitmapIdx(long handle) {
        return (int) (handle >>> Integer.SIZE);
    }

    @Override
    public int chunkSize() {
        return chunkSize;
    }

    @Override
    public int freeBytes() {
        synchronized (arena) {
            return freeBytes;
        }
    }

    @Override
    public String toString() {
        final int freeBytes;
        synchronized (arena) {
            freeBytes = this.freeBytes;
        }

        return new StringBuilder()
                .append("Chunk(")
                .append(Integer.toHexString(System.identityHashCode(this)))
                .append(": ")
                .append(usage(freeBytes))
                .append("%, ")
                .append(chunkSize - freeBytes)
                .append('/')
                .append(chunkSize)
                .append(')')
                .toString();
    }

    void destroy() {
        arena.destroyChunk(this);
    }
}
