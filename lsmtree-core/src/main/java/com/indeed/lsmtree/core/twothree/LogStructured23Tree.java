package com.indeed.lsmtree.core.twothree;

import com.google.common.base.Charsets;
import com.google.common.collect.AbstractIterator;
import com.google.common.io.Files;
import com.indeed.lsmtree.core.Generation;
import com.indeed.lsmtree.core.TransactionLog;
import com.indeed.util.core.shell.PosixFileOperations;
import com.indeed.util.mmap.DynamicMMapBufferDataOutputStream;
import com.indeed.util.mmap.Memory;
import com.indeed.util.mmap.MemoryDataInput;
import com.indeed.util.serialization.IntSerializer;
import com.indeed.util.serialization.LongSerializer;
import com.indeed.util.serialization.Serializer;
import fj.F3;
import org.apache.commons.collections.comparators.ComparableComparator;
import org.apache.log4j.Logger;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Comparator;
import java.util.Iterator;

/**
* @author jplaisance
*/
public final class LogStructured23Tree<K,V> implements Generation<K,V> {

    private static final Logger log = Logger.getLogger(LogStructured23Tree.class);

    private final File path;

    private final File rootPtrFile;
    private final DynamicMMapBufferDataOutputStream buffer;
    private int rootPtr = -1;

    private final Serializer<K> keySerializer;

    private final Serializer<V> valueSerializer;

    private final Comparator<K> comparator;

    private final TreeNode.Matcher<IOException> writeNodeMatcher = new TreeNode.Matcher<IOException>() {
        @Override
        IOException node2(final int keyPtr, final int valuePtr, final int leftPtr, final int rightPtr) {
            try {
                buffer.writeByte(1);
                buffer.writeInt(keyPtr);
                buffer.writeInt(valuePtr);
                buffer.writeInt(leftPtr);
                buffer.writeInt(rightPtr);
            } catch (IOException e) {
                return e;
            }
            return null;
        }

        @Override
        IOException node3(
                final int leftKeyPtr,
                final int rightKeyPtr,
                final int leftValuePtr,
                final int rightValuePtr,
                final int leftPtr,
                final int middlePtr,
                final int rightPtr
        ) {
            try {
                buffer.writeByte(2);
                buffer.writeInt(leftKeyPtr);
                buffer.writeInt(rightKeyPtr);
                buffer.writeInt(leftValuePtr);
                buffer.writeInt(rightValuePtr);
                buffer.writeInt(leftPtr);
                buffer.writeInt(middlePtr);
                buffer.writeInt(rightPtr);
            } catch (IOException e) {
                return e;
            }
            return null;
        }
    };

    public LogStructured23Tree(File path, Serializer<K> keySerializer, Serializer<V> valueSerializer, Comparator<K> comparator) throws IOException {
        this.path = path;
        path.mkdirs();
        rootPtrFile = new File(path, "rootptr");
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.comparator = comparator;
        final File dataFile = new File(path, "data");
        if (dataFile.exists()) {
            buffer = new DynamicMMapBufferDataOutputStream(dataFile, ByteOrder.LITTLE_ENDIAN, dataFile.length());
            MemoryDataInput in = new MemoryDataInput(buffer.memory());
            if (rootPtrFile.exists()) {
                rootPtr = Integer.parseInt(Files.readFirstLine(rootPtrFile, Charsets.UTF_8));
                //seek past root node
                in.seek(rootPtr);
                byte rootType = in.readByte();
                if (rootType == 1) {
                    in.seek(in.position()+17);
                } else if (rootType == 2) {
                    in.seek(in.position()+29);
                } else {
                    throw new IllegalStateException();
                }
            } else {
                rootPtr = -1;
            }
            byte type = 0;
            K key = null;
            V value = null;
            try {
                //read writes after last sync
                while (true) {
                    type = 0;
                    key = null;
                    value = null;
                    type = in.readByte();
                    if (type == 0) {
                        throw new EOFException();
                    } else if (type == 1) {
                        //put
                        key = keySerializer.read(in);
                        value = valueSerializer.read(in);
                        readTreePath(in);
                    } else if (type == 2) {
                        //delete
                        key = keySerializer.read(in);
                        readTreePath(in);
                    } else {
                        //garbage data
                        throw new IllegalStateException();
                    }
                }
            } catch (Exception e) {
                log.error("error", e);
                if (type == 1 && key != null && value != null) {
                    put(key, value);
                }
                if (type == 2 && key != null) {
                    delete(key);
                }
            }
            sync();
        } else {
            buffer = new DynamicMMapBufferDataOutputStream(dataFile, ByteOrder.LITTLE_ENDIAN);
        }
    }

    private void readTreePath(final MemoryDataInput in) throws IOException {
        int newRootPtr = -1;
        byte hasNext = in.readByte();
        for (; hasNext == 1; hasNext = in.readByte()) {
            newRootPtr = (int)in.position();
            byte nodeType = in.readByte();
            if (nodeType == 0) {
                throw new EOFException();
            } else if (nodeType == 1) {
                //Node2
                in.seek(in.position()+16);
            } else if (nodeType == 2) {
                //Node3
                in.seek(in.position()+28);
            } else {
                //garbage data
                throw new IllegalStateException(String.valueOf(nodeType));
            }
        }
        if (hasNext == 2) {
            rootPtr = newRootPtr;
        } else {
            throw new EOFException();
        }
    }

    private int writeKey(K key) {
        int ret = (int)buffer.position();
        try {
            keySerializer.write(key, buffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return ret;
    }

    private int writeValue(V value) {
        int ret = (int)buffer.position();
        try {
            valueSerializer.write(value, buffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return ret;
    }

    private K readKey(MemoryDataInput in, int address) {
        in.seek(address);
        try {
            return keySerializer.read(in);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private V readValue(MemoryDataInput in, int address) {
        in.seek(address);
        try {
            return valueSerializer.read(in);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private int writeNode(TreeNode node) {
        int ret = (int)buffer.position();
        node.match(writeNodeMatcher);
        return ret;
    }

    private TreeNode readNode(int address) {
        final Memory memory = buffer.memory();
        byte type = memory.getByte(address);
        if (type == 1) {
            int keyPtr = memory.getInt(address+1);
            int valuePtr = memory.getInt(address + 5);
            int leftPtr = memory.getInt(address + 9);
            int rightPtr = memory.getInt(address + 13);
            return new TreeNode.Node2(keyPtr, valuePtr, leftPtr, rightPtr);
        }
        if (type == 2) {
            int leftKeyPtr = memory.getInt(address+1);
            int rightKeyPtr = memory.getInt(address + 5);
            int leftValuePtr = memory.getInt(address + 9);
            int rightValuePtr = memory.getInt(address + 13);
            int leftPtr = memory.getInt(address + 17);
            int middlePtr = memory.getInt(address + 21);
            int rightPtr = memory.getInt(address + 25);
            return new TreeNode.Node3(leftKeyPtr, rightKeyPtr, leftValuePtr, rightValuePtr, leftPtr, middlePtr, rightPtr);
        }
        throw new IllegalStateException("bad data");
    }

    private <A> A insertionPath(K key, final F3<A, Integer, TreeNode, A> f, final A value) {
        if (rootPtr < 0) return value;
        TreeNode root = readNode(rootPtr);
        return insertionPath(key, root, f, value, new MemoryDataInput(buffer.memory()));
    }

    private <A> A insertionPath(
            final K searchKey,
            final TreeNode current,
            final F3<A, Integer, TreeNode, A> f,
            final A value,
            final MemoryDataInput in
    ) {
        return current.match(new TreeNode.Matcher<A>() {
            @Override
            A node2(
                    final int keyPtr,
                    final int valuePtr,
                    final int leftPtr,
                    final int rightPtr
            ) {
                final K key = readKey(in, keyPtr);
                final int cmp = Integer.signum(comparator.compare(searchKey, key));
                return insertionPath(searchKey, cmp, leftPtr, rightPtr, 1, current, f, value, in);
            }

            @Override
            A node3(
                    final int leftKeyPtr,
                    final int rightKeyPtr,
                    final int leftValuePtr,
                    final int rightValuePtr,
                    final int leftPtr,
                    final int middlePtr,
                    final int rightPtr
            ) {
                    final K leftKey = readKey(in, leftKeyPtr);
                    final int cmp1 = comparator.compare(searchKey, leftKey);
                    if (cmp1 <= 0) {
                        return insertionPath(searchKey, cmp1, leftPtr, -1, 1, current, f, value, in);
                    }
                    final K rightKey = readKey(in, rightKeyPtr);
                    final int cmp2 = Integer.signum(comparator.compare(searchKey, rightKey));
                    return insertionPath(searchKey, cmp2, middlePtr, rightPtr, 3, current, f, value, in);
            }
        });
    }

    private <A> A insertionPath(
            K searchKey,
            int cmp,
            int leftPtr,
            int rightPtr,
            int adjust,
            TreeNode current,
            final F3<A, Integer, TreeNode, A> f,
            final A value,
            MemoryDataInput in
    ) {
        A nextValue = value;
        if (cmp != 0) {
            int nextPtr = cmp < 0 ? leftPtr : rightPtr;
            final TreeNode next = nextPtr < 0 ? null : readNode(nextPtr);
            if (next != null) {
                nextValue = insertionPath(searchKey, next, f, nextValue, in);
            }
        }
        return f.f(nextValue, cmp+adjust, current);
    }

    private static interface Push {

        public <Z> Z match(Matcher<Z> matcher);

        static abstract class Matcher<Z> {
            Z replace(int newNodePtr) {
                return otherwise();
            }

            Z insert(int keyPtr, int valuePtr, int leftPtr, int rightPtr) {
                return otherwise();
            }

            Z otherwise() {
                throw new UnsupportedOperationException();
            }
        }
    }

    private static final class Replace implements Push {

        final int newNodePtr;

        private Replace(final int newNodePtr) {
            this.newNodePtr = newNodePtr;
        }

        @Override
        public <Z> Z match(final Matcher<Z> matcher) {
            return matcher.replace(newNodePtr);
        }
    }

    private static final class Insert implements Push {

        final int keyPtr;
        final int valuePtr;
        final int leftPtr;
        final int rightPtr;

        private Insert(final int keyPtr, final int valuePtr, final int leftPtr, final int rightPtr) {
            this.keyPtr = keyPtr;
            this.valuePtr = valuePtr;
            this.leftPtr = leftPtr;
            this.rightPtr = rightPtr;
        }

        @Override
        public <Z> Z match(final Matcher<Z> matcher) {
            return matcher.insert(keyPtr, valuePtr, leftPtr, rightPtr);
        }
    }

    public void put(final K key, final V value) {
        try {
            buffer.writeByte(1);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        final int newKeyPtr = writeKey(key);
        final int newValuePtr = writeValue(value);
        insert(key, new Insert(newKeyPtr, newValuePtr, -1, -1));
    }

    private F3<Push, Integer, TreeNode, Push> insertF = new F3<Push, Integer, TreeNode, Push>() {
        @Override
        public Push f(final Push push, final Integer insertionIndex, final TreeNode current) {
            try {
                buffer.writeByte(1);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return current.match(
                    new TreeNode.Matcher<Push>() {
                        @Override
                        Push node2(final int keyPtr, final int valuePtr, final int leftPtr, final int rightPtr) {
                            return push.match(new Push.Matcher<Push>() {
                                @Override
                                Push replace(final int newNodePtr) {
                                    final TreeNode.Node2 newNode;
                                    if (insertionIndex == 0) {
                                        newNode = new TreeNode.Node2(keyPtr, valuePtr, newNodePtr, rightPtr);
                                    } else {
                                        newNode = new TreeNode.Node2(keyPtr, valuePtr, leftPtr, newNodePtr);
                                    }
                                    return new Replace(writeNode(newNode));
                                }

                                @Override
                                Push insert(final int newKeyPtr, final int newValuePtr, final int newLeftPtr, final int newRightPtr) {
                                    if (insertionIndex == 1) {
                                        final TreeNode.Node2 newNode = new TreeNode.Node2(newKeyPtr, newValuePtr, leftPtr, rightPtr);
                                        return new Replace(writeNode(newNode));
                                    }
                                    final TreeNode.Node3 newNode;
                                    if (insertionIndex == 0) {
                                        newNode = new TreeNode.Node3(newKeyPtr, keyPtr, newValuePtr, valuePtr, newLeftPtr, newRightPtr, rightPtr);
                                    } else {
                                        newNode = new TreeNode.Node3(keyPtr, newKeyPtr, valuePtr, newValuePtr, leftPtr, newLeftPtr, newRightPtr);
                                    }
                                    return new Replace(writeNode(newNode));
                                }
                            });
                        }

                        @Override
                        Push node3(
                                final int leftKeyPtr,
                                final int rightKeyPtr,
                                final int leftValuePtr,
                                final int rightValuePtr,
                                final int leftPtr,
                                final int middlePtr,
                                final int rightPtr
                        ) {
                            return push.match(new Push.Matcher<Push>() {
                                @Override
                                Push replace(final int newNodePtr) {
                                    final TreeNode.Node3 newNode;
                                    if (insertionIndex == 0) {
                                        newNode = new TreeNode.Node3(leftKeyPtr, rightKeyPtr, leftValuePtr, rightValuePtr, newNodePtr, middlePtr, rightPtr);
                                    } else if (insertionIndex == 2) {
                                        newNode = new TreeNode.Node3(leftKeyPtr, rightKeyPtr, leftValuePtr, rightValuePtr, leftPtr, newNodePtr, rightPtr);
                                    } else {
                                        if (insertionIndex != 4) {
                                            throw new IllegalStateException();
                                        }
                                        newNode = new TreeNode.Node3(leftKeyPtr, rightKeyPtr, leftValuePtr, rightValuePtr, leftPtr, middlePtr, newNodePtr);
                                    }
                                    return new Replace(writeNode(newNode));
                                }

                                @Override
                                Push insert(final int newKeyPtr, final int newValuePtr, final int newLeftPtr, final int newRightPtr) {
                                    if ((insertionIndex & 1) == 1) {
                                        final TreeNode.Node3 newNode;
                                        if (insertionIndex == 1) {
                                            newNode = new TreeNode.Node3(newKeyPtr, rightKeyPtr, newValuePtr, rightValuePtr, leftPtr, middlePtr, rightPtr);
                                        } else {
                                            newNode = new TreeNode.Node3(leftKeyPtr, newKeyPtr, leftValuePtr, newValuePtr, leftPtr, middlePtr, rightPtr);
                                        }
                                        return new Replace(writeNode(newNode));
                                    } else {
                                        final TreeNode.Node2 leftNode;
                                        final TreeNode.Node2 rightNode;
                                        final int splitKeyPtr;
                                        final int splitValuePtr;
                                        if (insertionIndex == 0) {
                                            leftNode = new TreeNode.Node2(newKeyPtr, newValuePtr, newLeftPtr, newRightPtr);
                                            rightNode = new TreeNode.Node2(rightKeyPtr, rightValuePtr, middlePtr, rightPtr);
                                            splitKeyPtr = leftKeyPtr;
                                            splitValuePtr = leftValuePtr;
                                        } else if (insertionIndex == 2) {
                                            leftNode = new TreeNode.Node2(leftKeyPtr, leftValuePtr, leftPtr, newLeftPtr);
                                            rightNode = new TreeNode.Node2(rightKeyPtr, rightValuePtr, newRightPtr, rightPtr);
                                            splitKeyPtr = newKeyPtr;
                                            splitValuePtr = newValuePtr;
                                        } else {
                                            leftNode = new TreeNode.Node2(leftKeyPtr, leftValuePtr, leftPtr, middlePtr);
                                            rightNode = new TreeNode.Node2(newKeyPtr, newValuePtr, newLeftPtr, newRightPtr);
                                            splitKeyPtr = rightKeyPtr;
                                            splitValuePtr = rightValuePtr;
                                        }
                                        final int leftPtr1 = writeNode(leftNode);
                                        try {
                                            buffer.writeByte(1);
                                        } catch (IOException e) {
                                            throw new RuntimeException(e);
                                        }
                                        final int rightPtr1 = writeNode(rightNode);
                                        return new Insert(splitKeyPtr, splitValuePtr, leftPtr1, rightPtr1);
                                    }
                                }
                            });
                        }
                    }
            );
        }
    };

    private final Push.Matcher<Integer> insertPushRootMatcher = new Push.Matcher<Integer>() {
        @Override
        Integer replace(final int newNodePtr) {
            return newNodePtr;
        }

        @Override
        Integer insert(final int keyPtr, final int valuePtr, final int leftPtr, final int rightPtr) {
            try {
                buffer.writeByte(1);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            final TreeNode.Node2 newRoot = new TreeNode.Node2(keyPtr, valuePtr, leftPtr, rightPtr);
            return writeNode(newRoot);
        }
    };

    private void insert(final K key, Insert insert) {
        Push push = insertionPath(key, insertF, insert);
        rootPtr = push.match(insertPushRootMatcher);
        try {
            buffer.writeByte(2);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void delete(final K key) {
        try {
            buffer.writeByte(2);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        final int newKeyPtr = writeKey(key);
        insert(key, new Insert(newKeyPtr, -1, -1, -1));
    }

    @Override
    public Entry<K, V> get(final K key) {
        final MemoryDataInput in = new MemoryDataInput(buffer.memory());
        return insertionPath(key, new F3<Entry<K, V>, Integer, TreeNode, Entry<K, V>>() {
            @Override
            public Entry<K, V> f(final Entry<K, V> entry, final Integer insertionIndex, final TreeNode treeNode) {
                if (entry != null) {
                    return entry;
                }
                if ((insertionIndex & 1) == 0) {
                    return null;
                }
                return treeNode.match(new TreeNode.Matcher<Entry<K, V>>() {
                    @Override
                    Entry<K, V> node2(final int keyPtr, final int valuePtr, final int leftPtr, final int rightPtr) {
                        if (valuePtr < 0) {
                            return Entry.createDeleted(readKey(in, keyPtr));
                        } else {
                            return Entry.create(readKey(in, keyPtr), readValue(in, valuePtr));
                        }
                    }

                    @Override
                    Entry<K, V> node3(
                            final int leftKeyPtr,
                            final int rightKeyPtr,
                            final int leftValuePtr,
                            final int rightValuePtr,
                            final int leftPtr,
                            final int middlePtr,
                            final int rightPtr
                    ) {
                        if (insertionIndex == 1) {
                            if (leftKeyPtr < 0) {
                                return Entry.createDeleted(readKey(in, leftKeyPtr));
                            } else {
                                return Entry.create(readKey(in, leftKeyPtr), readValue(in, leftValuePtr));
                            }
                        } else {
                            if (rightValuePtr < 0) {
                                return Entry.createDeleted(readKey(in, rightKeyPtr));
                            } else {
                                return Entry.create(readKey(in, rightKeyPtr), readValue(in, rightValuePtr));
                            }
                        }
                    }
                });
            }
        }, null);
    }

    @Override
    public Boolean isDeleted(final K key) {
        final Entry<K, V> entry = get(key);
        return entry == null ? null : (entry.isDeleted() ? Boolean.TRUE : Boolean.FALSE);
    }

    @Override
    public Generation<K, V> head(final K end, final boolean inclusive) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Generation<K, V> tail(final K start, final boolean inclusive) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Generation<K, V> slice(final K start, final boolean startInclusive, final K end, final boolean endInclusive) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Generation<K, V> reverse() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        MemoryDataInput in = new MemoryDataInput(buffer.memory());
        Stack<NodeElement<K,V>> stack = rootPtr < 0 ? new Empty<NodeElement<K, V>>() : pushPreorderL(readNode(rootPtr), new Empty<NodeElement<K, V>>(), in);
        return new IteratorImpl(stack, pushPreorderL, in);
    }

    @Override
    public Iterator<Entry<K, V>> iterator(final K start, final boolean startInclusive) {
        throw new UnsupportedOperationException();
    }

    private static interface NodeElement<K,V> {

        public <Z> Z match(Matcher<K,V,Z> matcher);

        static abstract class Matcher<K,V,Z> {
            public Z entry(K key, V value, boolean deleted) {
                return otherwise();
            }

            public Z pointer(int address) {
                return otherwise();
            }

            public Z otherwise() {
                throw new UnsupportedOperationException();
            }
        }
    }

    private static class NodeEntry<K,V> implements NodeElement<K,V> {

        final K key;

        final V value;

        final  boolean deleted;

        private NodeEntry(final K key, final V value, final boolean deleted) {
            this.key = key;
            this.value = value;
            this.deleted = deleted;
        }

        @Override
        public <Z> Z match(final Matcher<K, V, Z> matcher) {
            return matcher.entry(key, value, deleted);
        }
    }

    private static class NodePointer<K,V> implements NodeElement<K,V> {

        final int address;

        private NodePointer(final int address) {
            this.address = address;
        }

        @Override
        public <Z> Z match(final Matcher<K, V, Z> matcher) {
            return matcher.pointer(address);
        }
    }

    private final class IteratorImpl extends AbstractIterator<Entry<K,V>> {

        private Stack<NodeElement<K,V>> state;

        private final F3<TreeNode, Stack<NodeElement<K,V>>, MemoryDataInput, Stack<NodeElement<K,V>>> pushF;

        private final MemoryDataInput in;

        private final Stack.Matcher<NodeElement<K,V>, Entry<K,V>> matcher = new Stack.Matcher<NodeElement<K, V>, Entry<K, V>>() {
            @Override
            Entry<K, V> empty() {
                return endOfData();
            }

            @Override
            Entry<K, V> cons(final NodeElement<K,V> head, final Stack<NodeElement<K,V>> tail) {
                return head.match(new NodeElement.Matcher<K, V, Entry<K,V>>() {
                    @Override
                    public Entry<K, V> entry(final K key, final V value, final boolean deleted) {
                        state = tail;
                        if (deleted) {
                            return Entry.createDeleted(key);
                        } else {
                            return Entry.create(key, value);
                        }
                    }

                    @Override
                    public Entry<K, V> pointer(final int address) {
                        state = pushF.f(readNode(address), tail, in);
                        return computeNext();
                    }
                });
            }
        };

        private IteratorImpl(
                Stack<NodeElement<K, V>> state,
                F3<TreeNode, Stack<NodeElement<K, V>>, MemoryDataInput, Stack<NodeElement<K, V>>> pushF,
                final MemoryDataInput in
        ) {
            this.state = state;
            this.pushF = pushF;
            this.in = in;
        }

        @Override
        protected Entry<K, V> computeNext() {
            return state.match(matcher);
        }
    }

    private F3<TreeNode, Stack<NodeElement<K,V>>, MemoryDataInput, Stack<NodeElement<K,V>>> pushPreorderL = new F3<TreeNode, Stack<NodeElement<K,V>>, MemoryDataInput, Stack<NodeElement<K,V>>>() {
        @Override
        public Stack<NodeElement<K, V>> f(
                final TreeNode treeNode, final Stack<NodeElement<K, V>> nodeElementStack, MemoryDataInput in
        ) {
            return pushPreorderL(treeNode, nodeElementStack, in);
        }
    };

    private Stack<NodeElement<K,V>> pushPreorderL(TreeNode node, final Stack<NodeElement<K,V>> stack, final MemoryDataInput in) {
        return node.match(new TreeNode.Matcher<Stack<NodeElement<K,V>>>() {
            @Override
            Stack<NodeElement<K, V>> node2(final int keyPtr, final int valuePtr, final int leftPtr, final int rightPtr) {
                Stack<NodeElement<K,V>> ret = stack;
                if (rightPtr >= 0) ret = new Cons<NodeElement<K, V>>(new NodePointer<K, V>(rightPtr), ret);
                final boolean deleted = valuePtr < 0;
                ret = new Cons<NodeElement<K, V>>(new NodeEntry<K, V>(readKey(in, keyPtr), deleted ? null : readValue(in, valuePtr), deleted), ret);
                if (leftPtr >= 0) ret = new Cons<NodeElement<K, V>>(new NodePointer<K, V>(leftPtr), ret);
                return ret;
            }

            @Override
            Stack<NodeElement<K, V>> node3(
                    final int leftKeyPtr,
                    final int rightKeyPtr,
                    final int leftValuePtr,
                    final int rightValuePtr,
                    final int leftPtr,
                    final int middlePtr,
                    final int rightPtr
            ) {
                Stack<NodeElement<K,V>> ret = stack;
                if (rightPtr >= 0) ret = new Cons<NodeElement<K, V>>(new NodePointer<K, V>(rightPtr), ret);
                final boolean rightDeleted = rightValuePtr < 0;
                ret = new Cons<NodeElement<K, V>>(new NodeEntry<K, V>(readKey(in, rightKeyPtr), rightDeleted ? null : readValue(in, rightValuePtr), rightDeleted), ret);
                if (middlePtr >= 0) ret = new Cons<NodeElement<K, V>>(new NodePointer<K, V>(middlePtr), ret);
                final boolean leftDeleted = leftValuePtr < 0;
                ret = new Cons<NodeElement<K, V>>(new NodeEntry<K, V>(readKey(in, leftKeyPtr), leftDeleted ? null : readValue(in, leftValuePtr), leftDeleted), ret);
                if (leftPtr >= 0) ret = new Cons<NodeElement<K, V>>(new NodePointer<K, V>(leftPtr), ret);
                return ret;
            }
        });
    }

    private F3<TreeNode, Stack<NodeElement<K,V>>, MemoryDataInput, Stack<NodeElement<K,V>>> pushPreorderR = new F3<TreeNode, Stack<NodeElement<K,V>>, MemoryDataInput, Stack<NodeElement<K,V>>>() {
        @Override
        public Stack<NodeElement<K, V>> f(
                final TreeNode treeNode, final Stack<NodeElement<K, V>> nodeElementStack, MemoryDataInput in
        ) {
            return pushPreorderR(treeNode, nodeElementStack, in);
        }
    };

    private Stack<NodeElement<K,V>> pushPreorderR(TreeNode node, final Stack<NodeElement<K,V>> stack, final MemoryDataInput in) {
        return node.match(new TreeNode.Matcher<Stack<NodeElement<K,V>>>() {
            @Override
            Stack<NodeElement<K, V>> node2(final int keyPtr, final int valuePtr, final int leftPtr, final int rightPtr) {
                Stack<NodeElement<K,V>> ret = stack;
                if (leftPtr >= 0) ret = new Cons<NodeElement<K, V>>(new NodePointer<K, V>(leftPtr), ret);
                final boolean deleted = valuePtr < 0;
                ret = new Cons<NodeElement<K, V>>(new NodeEntry<K, V>(readKey(in, keyPtr), deleted ? null : readValue(in, valuePtr), deleted), ret);
                if (rightPtr >= 0) ret = new Cons<NodeElement<K, V>>(new NodePointer<K, V>(rightPtr), ret);
                return ret;
            }

            @Override
            Stack<NodeElement<K, V>> node3(
                    final int leftKeyPtr,
                    final int rightKeyPtr,
                    final int leftValuePtr,
                    final int rightValuePtr,
                    final int leftPtr,
                    final int middlePtr,
                    final int rightPtr
            ) {
                Stack<NodeElement<K,V>> ret = stack;
                if (leftPtr >= 0) ret = new Cons<NodeElement<K, V>>(new NodePointer<K, V>(leftPtr), ret);
                final boolean leftDeleted = leftValuePtr < 0;
                ret = new Cons<NodeElement<K, V>>(new NodeEntry<K, V>(readKey(in, leftKeyPtr), leftDeleted ? null : readValue(in, leftValuePtr), leftDeleted), ret);
                if (middlePtr >= 0) ret = new Cons<NodeElement<K, V>>(new NodePointer<K, V>(middlePtr), ret);
                final boolean rightDeleted = rightValuePtr < 0;
                ret = new Cons<NodeElement<K, V>>(new NodeEntry<K, V>(readKey(in, rightKeyPtr), rightDeleted ? null : readValue(in, rightValuePtr), rightDeleted), ret);
                if (rightPtr >= 0) ret = new Cons<NodeElement<K, V>>(new NodePointer<K, V>(rightPtr), ret);
                return ret;
            }
        });
    }

    @Override
    public Iterator<Entry<K, V>> reverseIterator() {
        MemoryDataInput in = new MemoryDataInput(buffer.memory());
        Stack<NodeElement<K,V>> stack = rootPtr < 0 ? new Empty<NodeElement<K, V>>() : pushPreorderR(readNode(rootPtr), new Empty<NodeElement<K, V>>(), in);
        return new IteratorImpl(stack, pushPreorderR, in);
    }

    @Override
    public Iterator<Entry<K, V>> reverseIterator(final K start, final boolean startInclusive) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long size() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long sizeInBytes() throws IOException {
        return buffer.position();
    }

    @Override
    public boolean hasDeletions() {
        return true;
    }

    @Override
    public File getPath() {
        return path;
    }

    @Override
    public Comparator<K> getComparator() {
        return comparator;
    }

    @Override
    public void delete() throws IOException {
        PosixFileOperations.rmrf(path);
    }

    @Override
    public void checkpoint(final File checkpointPath) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        try {
            sync();
        } finally {
            buffer.close();
        }
    }

    public void sync() throws IOException {
        buffer.sync();
        File tmpFile = File.createTempFile(rootPtrFile.getName(), ".tmp", path);
        Files.write(String.valueOf(rootPtr), tmpFile, Charsets.UTF_8);
        tmpFile.renameTo(rootPtrFile);
    }

    public void replay(File path) throws IOException {
        throw new UnsupportedOperationException();
    }

    public void closeWriter() throws IOException {

    }

    private static interface TreeNode {
        public <Z> Z match(Matcher<Z> matcher);

        static abstract class Matcher<Z> {

            Z node2(int keyPtr, int valuePtr, int leftPtr, int rightPtr) {
                return otherwise();
            }

            Z node3(int leftKeyPtr, int rightKeyPtr, int leftValuePtr, int rightValuePtr, int leftPtr, int middlePtr, int rightPtr) {
                return otherwise();
            }

            Z otherwise() {
                throw new UnsupportedOperationException();
            }
        }

        static final class Node2 implements TreeNode {

            final int keyPtr;
            final int valuePtr;
            final int leftPtr;
            final int rightPtr;

            private Node2(final int keyPtr, final int valuePtr, final int leftPtr, final int rightPtr) {
                this.keyPtr = keyPtr;
                this.valuePtr = valuePtr;
                this.leftPtr = leftPtr;
                this.rightPtr = rightPtr;
            }

            @Override
            public <Z> Z match(final Matcher<Z> matcher) {
                return matcher.node2(keyPtr, valuePtr, leftPtr, rightPtr);
            }
        }

        static final class Node3 implements TreeNode {

            final int leftKeyPtr;
            final int rightKeyPtr;
            final int leftValuePtr;
            final int rightValuePtr;
            final int leftPtr;
            final int middlePtr;
            final int rightPtr;

            private Node3(
                    final int leftKeyPtr,
                    final int rightKeyPtr,
                    final int leftValuePtr,
                    final int rightValuePtr,
                    final int leftPtr,
                    final int middlePtr,
                    final int rightPtr
            ) {
                this.leftKeyPtr = leftKeyPtr;
                this.rightKeyPtr = rightKeyPtr;
                this.leftValuePtr = leftValuePtr;
                this.rightValuePtr = rightValuePtr;
                this.leftPtr = leftPtr;
                this.middlePtr = middlePtr;
                this.rightPtr = rightPtr;
            }

            @Override
            public <Z> Z match(final Matcher<Z> matcher) {
                return matcher.node3(leftKeyPtr, rightKeyPtr, leftValuePtr, rightValuePtr, leftPtr, middlePtr, rightPtr);
            }
        }
    }

    private static interface Stack<T> {
        <Z> Z match(Matcher<T, Z> matcher);

        static abstract class Matcher<T,Z> {

            Z empty() {
                return otherwise();
            }

            Z cons(T head, Stack<T> tail) {
                return otherwise();
            }

            Z otherwise() {
                throw new UnsupportedOperationException();
            }
        }
    }

    private static final class Empty<T> implements Stack<T> {

        @Override
        public <Z> Z match(final Matcher<T, Z> matcher) {
            return matcher.empty();
        }
    }

    private static final class Cons<T> implements Stack<T> {

        final T head;
        final Stack<T> tail;

        Cons(final T head, final Stack<T> tail) {
            this.head = head;
            this.tail = tail;
        }

        @Override
        public <Z> Z match(final Matcher<T, Z> matcher) {
            return matcher.cons(head, tail);
        }
    }

    public static void main(String[] args) throws IOException, TransactionLog.LogClosedException {
        LogStructured23Tree<Integer, Long> tree = new LogStructured23Tree<Integer, Long>(new File("/scratch2/logstructered23treetest"),
                new IntSerializer(), new LongSerializer(),
                new ComparableComparator()
        );
//        for (int j = 0; j < 10; j++) {
//            LogStructured23Tree<Integer, Long> tree = new LogStructured23Tree<Integer, Long>(new File("/scratch2/logstructered23treetest"),
//                    new IntSerializer(), new LongSerializer(),
//                    new ComparableComparator()
//            );
//            long start = System.nanoTime();
//            for (int i = 0; i < 1024*1024; i++) {
//                tree.put(i, new Long(i));
//            }
//            System.out.println("2-3 put "+(System.nanoTime()-start)/1000000d+" ms");
//            start = System.nanoTime();
//            for (int i = 0; i < 1024*1024; i++) {
//                if (tree.get(i).getValue() != i) {
//                    System.out.println("ruh roh");
//                }
//            }
//            System.out.println("2-3 get "+(System.nanoTime()-start)/1000000d+" ms");
//            final File logPath = new File("/scratch2/logstructered23vgtest");
//            logPath.delete();
//            VolatileGeneration<Integer, Long> vg = new VolatileGeneration<Integer, Long>(logPath, new IntSerializer(), new LongSerializer(), new ComparableComparator(), false);
//            start = System.nanoTime();
//            for (int i = 0; i < 1024*1024; i++) {
//                vg.put(i, new Long(i));
//            }
//            System.out.println("vg put "+(System.nanoTime()-start)/1000000d+" ms");
//            start = System.nanoTime();
//            for (int i = 0; i < 1024*1024; i++) {
//                if (vg.get(i).getValue() != i) {
//                    System.out.println("ruh roh");
//                }
//            }
//            System.out.println("vg get "+(System.nanoTime()-start)/1000000d+" ms");
//        }
//        System.exit(0);
//        for (int i = 0; i < 1024; i++) {
//            tree.put(i, new Long(i));
//        }
//        for (int i = 0; i < 1024*128; i++) {
//            Entry<Integer, Long> entry = tree.get(i);
//            if (entry == null || entry.getKey() != i || entry.isDeleted() || entry.getValue() != i) {
//                System.out.println(i+" ruh roh 1: "+entry);
//            }
//        }
//        for (int i = 0; i < 1024; i+=2) {
//            tree.delete(i);
//        }
//        for (int i = 0; i < 1024*128; i++) {
//            Entry<Integer, Long> entry = tree.get(i);
//            if (i % 2 == 0) {
//                if (entry == null || entry.getKey() != i || !entry.isDeleted()) {
//                    System.out.println(i+" ruh roh 2: "+entry);
//                }
//            } else {
//                if (entry == null || entry.getKey() != i || entry.isDeleted() || entry.getValue() != i) {
//                    System.out.println(i+" ruh roh 3: "+entry);
//                }
//            }
//        }
//        for (int i = 0; i < 1024*128; i++) {
//            tree.put(i, new Long(i));
//        }
        Iterator<Entry<Integer, Long>> iterator = tree.iterator();
        while (iterator.hasNext()) {
//            iterator.next();
            System.out.println(iterator.next());
        }
        tree.close();
    }
}
