package com.github.orm.elasticsearch.core.base;
import com.github.orm.elasticsearch.core.annotation.ESId;
import com.github.orm.elasticsearch.core.enums.ESFieldType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.experimental.Accessors;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * @ClassName ESClassLoopMapping
 * @Description 深度实体映射解析循环版
 * @Author liyongbing
 * @Date 2022/7/29 11:55
 * @Version 1.0
 **/
public class ESClassLoopMapping {

    private Class<?> type;
    private XContentBuilder builder;
    private Deque<Node> stack = new ArrayDeque<>();


    public ESClassLoopMapping(Class<?> type) throws IOException {
        this.type = type;
        builder = XContentFactory.jsonBuilder();
    }

    public ESClassLoopMapping(Class<?> type, XContentBuilder builder) {
        this.type = type;
        this.builder = builder;
    }

    public ESClassLoopMapping mapping() throws IOException {
        Node root = new Node(0, type);
        stack.push(root);
        while (!stack.isEmpty()) {
            Node node = stack.getFirst();
            if (node.isWaitClosed()) {
                endObject();
            } else {
                startObject(node);
                node.setWaitClosed(true);
            }
        }
        return this;
    }

    private void startObject(Node node) throws IOException {
        if (0 == node.getNodeType()) {
            builder.startObject();
        } else {
            builder.startObject(node.getName());
            builder.field("type", node.getData().getFieldType().typeName);
        }
        builder.startObject("properties");
        Class last = node.getClazz();
        Field[] fields = last.getDeclaredFields();
        for (Field field : fields) {
            ESId esId = field.getAnnotation(ESId.class);
            if (esId != null) {
                continue;
            }
            ReflectionUtils.ESFieldData data = ReflectionUtils.getESFieldData(field);
            if (data.getFieldType() == null) {
                continue;
            }
            if (ESFieldType.OBJECT.equals(data.getFieldType()) || ESFieldType.NESTED.equals(data.getFieldType())) {
                objectField(node, field, data);
            } else {
                basicField(field, data);
            }
        }
    }

    private void endObject() throws IOException {
        builder.endObject();
        builder.endObject();
        stack.pop();
    }


    private void objectField(Node node, Field field, ReflectionUtils.ESFieldData data) {
        Node e = new Node(1, ReflectionUtils.getTypeOrCollectionRealType(field)).setName(field.getName()).setParent(node).setData(data);
        Node current = e;
        while (current.getParent() != null) {
            if (e.getName().equals(current.getParent().getName()) && e.getClazz().equals(current.getParent().getClazz())) {
                return;
            }
            current = current.getParent();
        }
        stack.push(e);
    }

    private void basicField(Field field, ReflectionUtils.ESFieldData data) throws IOException {
        builder.startObject(field.getName());
        builder.field("type", data.getFieldType().typeName);
        String analyzer = data.getAnalyzer();
        if (StringUtils.hasText(analyzer)) {
            builder.field("analyzer", analyzer);
        }
        if (data.isTextRaw()) {
            builder.startObject("fields");
            builder.startObject(data.getTextRawName());
            builder.field("type", ESFieldType.KEYWORD.typeName);
            int ignoreAbove = data.getTextRawIgnoreAbove();
            if (ignoreAbove > 0) {
                builder.field("ignore_above", ignoreAbove);
            }
            builder.endObject();
            builder.endObject();
        }
        builder.endObject();
    }

    public XContentBuilder getBuilder() {
        return builder;
    }


    @AllArgsConstructor
    @Accessors(chain = true)
    @Data
    private static class Node {
        private int nodeType;
        private Class clazz;
        private String name;
        private ReflectionUtils.ESFieldData data;
        private boolean waitClosed;
        private Node parent;

        public Node(int nodeType, Class clazz) {
            this.nodeType = nodeType;
            this.clazz = clazz;
        }

        @Override
        public boolean equals(Object o) {
            return super.equals(o);
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }
    }

}
