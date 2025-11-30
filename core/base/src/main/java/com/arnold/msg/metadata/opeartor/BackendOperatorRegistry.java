package com.arnold.msg.metadata.opeartor;

import com.arnold.msg.exceptions.NotInitializeException;
import com.arnold.msg.metadata.model.ClusterKind;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BackendOperatorRegistry {

    private static final Map<ClusterKind, BackendOperator> OPERATOR_MAP = new ConcurrentHashMap<>();

    public static void registerOperator(ClusterKind kind, BackendOperator operator) {
        OPERATOR_MAP.put(kind, operator);
    }

    public static BackendOperator getOperator(ClusterKind kind) {
        if (!OPERATOR_MAP.containsKey(kind)) {
            throw new NotInitializeException("Backend Operator has not been initialize with: " + kind);
        }
        return OPERATOR_MAP.get(kind);
    }
}
